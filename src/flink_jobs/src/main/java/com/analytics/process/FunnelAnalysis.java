package com.analytics.process;

import com.analytics.model.UserBehaviorEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashMap;
import java.util.Map;

/**
 * 实时漏斗分析
 *
 * 分析用户转化路径：曝光 → 点击 → 下载 → 安装
 *
 * 每一层的人数用一个计数器，然后用当前层 / 上一层 = 转化率。
 *
 * 这不是真正意义上"同一个用户"的漏斗（那需要 CEP 做事件匹配），
 * 而是一个每 10 分钟的"宏观漏斗"：这 10 分钟里发生了多少次曝光、
 * 多少次点击、多少次下载——算整体转化率。
 *
 * 面试时可以说明：
 * "Demo 里用的宏观漏斗（简单高效）；
 *  精确到用户级别的漏斗可以用 Flink CEP 做事件序列匹配。"
 */
public class FunnelAnalysis {

    /** 漏斗步骤定义（按顺序） */
    private static final String[] FUNNEL_STEPS = {
            "ad_impression",    // 第 1 步：曝光
            "ad_click",         // 第 2 步：点击
            "app_download",     // 第 3 步：下载
            "app_install"       // 第 4 步：安装
    };

    public static DataStream<String> process(DataStream<UserBehaviorEvent> sourceStream) {

        return sourceStream
                // Step 1: 只保留漏斗相关事件
                .filter((FilterFunction<UserBehaviorEvent>) event -> {
                    String type = event.getEventType();
                    for (String step : FUNNEL_STEPS) {
                        if (step.equals(type)) return true;
                    }
                    return false;
                })

                // Step 2: 事件类型 → (step_name, 1)
                .map((MapFunction<UserBehaviorEvent, org.apache.flink.api.java.tuple.Tuple2<String, Integer>>)
                        event -> org.apache.flink.api.java.tuple.Tuple2.of(event.getEventType(), 1))

                // Step 3: 10 分钟滚动窗口
                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(10)))

                // Step 4: 窗口内按事件类型计数
                .process(new FunnelWindowFunction());
    }

    /**
     * 窗口处理函数
     *
     * ProcessWindowFunction 可以对整个窗口的数据一次性处理，
     * 比 AggregateFunction 更灵活（但性能略差，用于小数据量场景）。
     */
    public static class FunnelWindowFunction
            extends org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction<
                    org.apache.flink.api.java.tuple.Tuple2<String, Integer>,
                    String,
                    org.apache.flink.streaming.api.windowing.windows.TimeWindow> {

        @Override
        public void process(
                org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction<
                        org.apache.flink.api.java.tuple.Tuple2<String, Integer>,
                        String,
                        org.apache.flink.streaming.api.windowing.windows.TimeWindow>.Context context,
                Iterable<org.apache.flink.api.java.tuple.Tuple2<String, Integer>> elements,
                org.apache.flink.util.Collector<String> out) {

            // 统计每种事件的数量
            Map<String, Long> stepCounts = new HashMap<>();
            for (String step : FUNNEL_STEPS) {
                stepCounts.put(step, 0L);
            }
            for (var e : elements) {
                stepCounts.merge(e.f0, 1L, Long::sum);
            }

            // 计算转化率
            long impressionCount = stepCounts.get("ad_impression");
            long clickCount = stepCounts.get("ad_click");
            long downloadCount = stepCounts.get("app_download");
            long installCount = stepCounts.get("app_install");

            StringBuilder json = new StringBuilder();
            json.append("{");
            json.append("\"metric\":\"funnel\",");
            json.append("\"impression\":").append(impressionCount).append(",");
            json.append("\"click\":").append(clickCount).append(",");
            json.append("\"download\":").append(downloadCount).append(",");
            json.append("\"install\":").append(installCount).append(",");

            // 转化率（相对于第一步）
            json.append("\"click_rate\":").append(rate(clickCount, impressionCount)).append(",");
            json.append("\"download_rate\":").append(rate(downloadCount, impressionCount)).append(",");
            json.append("\"install_rate\":").append(rate(installCount, impressionCount)).append(",");

            json.append("\"time\":\"").append(java.time.LocalDateTime.now()).append("\"");
            json.append("}");

            out.collect(json.toString());
        }

        private double rate(long numerator, long denominator) {
            if (denominator == 0) return 0.0;
            return Math.round(numerator * 10000.0 / denominator) / 100.0;
        }
    }
}
