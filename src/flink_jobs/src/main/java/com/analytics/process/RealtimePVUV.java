package com.analytics.process;

import com.analytics.model.UserBehaviorEvent;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashSet;
import java.util.Set;

/**
 * 实时 PV/UV 计算
 *
 * ┌────────────── 核心概念 ──────────────┐
 * │  PV (Page View):  页面浏览量          │ → 直接 count
 * │  UV (Unique Visitor): 独立访客        │ → 需要去重
 * │                                      │
 * │  去重方案：                            │
 * │  开发/Demo → HashSet 精确去重          │
 * │  生产环境 → HyperLogLog 近似去重       │
 * │  (误差 <2%，内存仅 KB 级 vs GB 级)     │
 * └──────────────────────────────────────┘
 *
 * HyperLogLog 原理（面试必问）：
 *   不存 user_id 原文，只存 user_id 的哈希值特征。
 *   通过"哈希值末尾连续0的个数"估算不同元素数量。
 *   类比：连续抛硬币 10 次都是正面 ≈ 抛了约 2^10 = 1024 次。
 *
 * 输出：每 5 分钟一条 → 写入 Redis / 打印日志
 */
public class RealtimePVUV {

    public static DataStream<String> process(DataStream<UserBehaviorEvent> sourceStream) {

        return sourceStream
                // Step 1: 每条事件 → (user_id, 1)
                .map(event -> Tuple2.of(event.getUserId(), 1))
                // 返回值类型帮助：Tuple2<String, Integer>

                // Step 2: 5 分钟滚动窗口
                // Tumbling = 窗口不重叠：[14:00-14:05), [14:05-14:10) ...
                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(5)))

                // Step 3: 窗口内聚合
                .aggregate(new PvUvAggregator());
    }

    /**
     * PV/UV 聚合器
     *
     * AggregateFunction<IN, ACC, OUT>:
     *   IN  = 输入: Tuple2<user_id, count>
     *   ACC = 累加器: PvUvAccumulator (每个窗口一个)
     *   OUT = 输出: JSON 字符串
     */
    public static class PvUvAggregator
            implements AggregateFunction<Tuple2<String, Integer>, PvUvAccumulator, String> {

        @Override
        public PvUvAccumulator createAccumulator() {
            return new PvUvAccumulator();
        }

        @Override
        public PvUvAccumulator add(Tuple2<String, Integer> value, PvUvAccumulator acc) {
            acc.pv += 1;
            acc.userIdSet.add(value.f0);   // HashSet 去重
            return acc;
        }

        @Override
        public String getResult(PvUvAccumulator acc) {
            return String.format(
                    "{\"metric\":\"pv_uv\",\"pv\":%d,\"uv\":%d,\"window_time\":\"%s\"}",
                    acc.pv,
                    acc.userIdSet.size(),
                    java.time.LocalDateTime.now().toString()
            );
        }

        @Override
        public PvUvAccumulator merge(PvUvAccumulator a, PvUvAccumulator b) {
            PvUvAccumulator merged = new PvUvAccumulator();
            merged.pv = a.pv + b.pv;
            merged.userIdSet.addAll(a.userIdSet);
            merged.userIdSet.addAll(b.userIdSet);
            return merged;
        }
    }

    /**
     * 窗口累加器 — 每个窗口一个实例
     */
    public static class PvUvAccumulator {
        public long pv = 0;
        public Set<String> userIdSet = new HashSet<>();
    }
}
