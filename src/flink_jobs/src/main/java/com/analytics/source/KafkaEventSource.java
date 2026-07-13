package com.analytics.source;

import com.analytics.model.UserBehaviorEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.Duration;

/**
 * Kafka 数据源工厂
 *
 * 负责创建 Flink 的 KafkaSource，从 Kafka Topic 消费用户行为日志。
 *
 * 关键设计：
 * 1. JSON 反序列化 → UserBehaviorEvent 对象
 * 2. Watermark 策略 → 处理乱序数据（允许最多 5 秒延迟）
 * 3. 从 latest offset 开始消费（只处理新到的数据）
 */
public class KafkaEventSource {

    // ObjectMapper 是 Jackson 的核心类，负责 JSON ↔ Java 对象互转
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * 创建 Kafka Source
     *
     * @param bootstrapServers  Kafka 地址，如 "localhost:9092"
     * @param topic             Topic 名称，如 "user-behavior-log"
     * @param consumerGroup     消费者组名，Flink 作业的唯一标识
     * @return KafkaSource<UserBehaviorEvent>
     */
    public static KafkaSource<UserBehaviorEvent> create(
            String bootstrapServers,
            String topic,
            String consumerGroup) {

        return KafkaSource.<UserBehaviorEvent>builder()
                // ── Kafka 连接配置 ──
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(consumerGroup)

                // ── 从最新数据开始消费（不处理历史数据） ──
                // 如果要从头消费，换成 OffsetsInitializer.earliest()
                .setStartingOffsets(OffsetsInitializer.latest())

                // ── JSON 反序列化：bytes → UserBehaviorEvent ──
                .setValueOnlyDeserializer(new AbstractDeserializationSchema<>() {
                    @Override
                    public UserBehaviorEvent deserialize(byte[] message) throws IOException {
                        return mapper.readValue(message, UserBehaviorEvent.class);
                    }
                })

                .build();
    }

    /**
     * Watermark 策略
     *
     * Watermark 是 Flink 处理乱序数据的核心机制。
     *
     * 场景：网络延迟导致 event_time=14:00:00 的数据，
     *       在 14:00:05 才到达 Flink。
     *
     * Watermark = "我假定所有早于 (当前最大event_time - 5秒) 的数据都到了"
     * 设 outOfOrdernessMillis=5000，意味着容忍 5 秒延迟。
     * 超过 5 秒的迟到数据会被丢弃或走旁路。
     *
     * 面试必考：Watermark 原理、怎么设置延迟时间。
     */
    public static WatermarkStrategy<UserBehaviorEvent> getWatermarkStrategy() {
        return WatermarkStrategy
                // 允许最多 5 秒乱序
                .<UserBehaviorEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                // 用 event_time 字段作为事件时间（这里简化处理，用处理时间代替）
                // 生产环境会用 eventTime 解析成时间戳
                .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis())
                // 空闲超时：如果 30 秒没收到数据，标记这个分区为空闲
                .withIdleness(Duration.ofSeconds(30));
    }
}
