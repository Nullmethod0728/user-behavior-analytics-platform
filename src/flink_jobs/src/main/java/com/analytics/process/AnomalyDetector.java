package com.analytics.process;

import com.analytics.model.UserBehaviorEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 异常检测告警
 *
 * 检测规则：如果最近 5 分钟的下载量比上一个 5 分钟下降超过 50%，触发告警。
 *
 * ┌──────── 算法原理 ────────┐
 * │                         │
 * │ 窗口1: [14:00-14:05) → 100次下载
 * │ 窗口2: [14:05-14:10) → 30次下载   │
 * │                         │
 * │ 变化率 = (30-100)/100 = -70%      │
 * │ -70% < -50% → 🔴 触发告警！       │
 * └─────────────────────────┘
 *
 * 面试可以提：生产环境用 Flink CEP (Complex Event Processing)
 * 来做更复杂的模式检测，比如"连续 3 个窗口下降且总降幅超 60%"
 */
public class AnomalyDetector {

    /** 告警阈值：下降超过 50% 触发 */
    private static final double ALERT_THRESHOLD = -0.50;

    /** 保存上一窗口的下载量，用于对比 */
    private static long previousCount = -1;

    public static DataStream<String> process(DataStream<UserBehaviorEvent> sourceStream) {

        return sourceStream
                // Step 1: 只关注下载事件
                .filter((FilterFunction<UserBehaviorEvent>)
                        event -> "app_download".equals(event.getEventType()))

                // Step 2: 每个下载 → 计数 1
                .map((MapFunction<UserBehaviorEvent, Integer>) event -> 1)

                // Step 3: 5 分钟滚动窗口，统计下载量
                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(5)))

                // Step 4: 窗口内求和 + 和上窗口对比
                .process(new AlertProcessFunction());
    }

    /**
     * 告警处理函数
     *
     * 合并了"计数"和"对比上一窗口"两步。
     */
    public static class AlertProcessFunction
            extends org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction<
                    Integer,
                    String,
                    org.apache.flink.streaming.api.windowing.windows.TimeWindow> {

        @Override
        public void process(
                Context context,
                Iterable<Integer> elements,
                org.apache.flink.util.Collector<String> out) {

            // 求和当前窗口的下载量
            long currentCount = 0;
            for (Integer i : elements) {
                currentCount += i;
            }

            // 对比上一窗口
            if (previousCount > 0) {
                double changeRate = (double) (currentCount - previousCount) / previousCount;

                if (changeRate < ALERT_THRESHOLD) {
                    // 🔴 触发告警
                    String alert = String.format(
                            "{\"type\":\"ALERT\",\"message\":\"下载量异常下降！\"," +
                            "\"previous\":%d,\"current\":%d,\"change_rate\":%.2f%%," +
                            "\"time\":\"%s\"}",
                            previousCount, currentCount,
                            changeRate * 100,
                            java.time.LocalDateTime.now().toString()
                    );
                    out.collect(alert);
                } else {
                    // 🟢 正常
                    String info = String.format(
                            "{\"type\":\"INFO\",\"message\":\"下载量正常\"," +
                            "\"current\":%d,\"change_rate\":%.2f%%}",
                            currentCount, changeRate * 100
                    );
                    out.collect(info);
                }
            }

            // 记录当前值，供下个窗口对比
            previousCount = currentCount;

            // 打印到控制台
            System.out.printf("[AnomalyDetector] 当前5分钟下载量: %d%n", currentCount);
        }
    }
}
