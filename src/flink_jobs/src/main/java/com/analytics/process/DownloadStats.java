package com.analytics.process;

import com.analytics.model.UserBehaviorEvent;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.api.common.typeinfo.TypeHint;

import java.util.Map;

/**
 * 实时下载量统计
 *
 * 按 App 维度统计下载量，每 1 分钟更新最近 5 分钟的下载排行。
 *
 * Sliding Window (滑动窗口)：适合做"滚动排行榜"
 *   [14:00────14:05]  窗口大小 5 分钟
 *        [14:01────14:06]  滑动步长 1 分钟
 *             [14:02────14:07]
 *
 * 和 Tumbling Window 的区别：
 *   Tumbling  = 窗口不重叠，每条数据只属于一个窗口
 *   Sliding   = 窗口有重叠，一条数据属于多个窗口
 *               适合做"最近N分钟的实时排行榜"
 */
public class DownloadStats {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static DataStream<String> process(DataStream<UserBehaviorEvent> sourceStream) {

        return sourceStream
                // Step 1: 只过滤出下载事件
                .filter((FilterFunction<UserBehaviorEvent>)
                        event -> "app_download".equals(event.getEventType()))

                // Step 2: 从 properties JSON 里提取 app_name
                .map(event -> {
                    String appName = "unknown";
                    try {
                        // properties 是 JSON 字符串: {"app_name":"微信",...}
                        @SuppressWarnings("unchecked")
                        Map<String, Object> props = mapper.readValue(
                                event.getProperties(), Map.class);
                        Object name = props.get("app_name");
                        if (name != null) {
                            appName = name.toString();
                        }
                    } catch (Exception ignored) {
                    }
                    // 输出: (app_name, 1)
                    return new org.apache.flink.api.java.tuple.Tuple2<>(appName, 1);
                })
                .returns(new TypeHint<org.apache.flink.api.java.tuple.Tuple2<String, Integer>>() {})

                // Step 3: 按 app_name 分组
                .keyBy(t -> t.f0)

                // Step 4: 滑动窗口：5 分钟窗口，每 1 分钟滑动一次
                .window(SlidingProcessingTimeWindows.of(
                        Time.minutes(5),   // 窗口大小
                        Time.minutes(1)))  // 滑动步长

                // Step 5: 每窗口内聚合
                .aggregate(new DownloadCountAggregator());
    }

    /**
     * 下载量聚合器
     */
    public static class DownloadCountAggregator
            implements AggregateFunction<
                    org.apache.flink.api.java.tuple.Tuple2<String, Integer>,
                    DownloadAccumulator,
                    String> {

        @Override
        public DownloadAccumulator createAccumulator() {
            return new DownloadAccumulator();
        }

        @Override
        public DownloadAccumulator add(
                org.apache.flink.api.java.tuple.Tuple2<String, Integer> value,
                DownloadAccumulator acc) {
            acc.appName = value.f0;  // 记录 app 名
            acc.count += 1;
            return acc;
        }

        @Override
        public String getResult(DownloadAccumulator acc) {
            return String.format(
                    "{\"metric\":\"download\",\"app\":\"%s\",\"count\":%d,\"time\":\"%s\"}",
                    acc.appName, acc.count,
                    java.time.LocalDateTime.now().toString()
            );
        }

        @Override
        public DownloadAccumulator merge(DownloadAccumulator a, DownloadAccumulator b) {
            DownloadAccumulator merged = new DownloadAccumulator();
            merged.appName = a.appName;
            merged.count = a.count + b.count;
            return merged;
        }
    }

    /** 下载量累加器 */
    public static class DownloadAccumulator {
        public String appName = "";
        public long count = 0;
    }
}
