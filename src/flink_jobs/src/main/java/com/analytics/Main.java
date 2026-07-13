package com.analytics;

import com.analytics.model.UserBehaviorEvent;
import com.analytics.process.*;
import com.analytics.sink.ClickHouseSink;
import com.analytics.sink.RedisSink;
import com.analytics.source.KafkaEventSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink 实时处理 — 主入口
 *
 * ┌────────── 数据流总图 ──────────┐
 * │                               │
 * │  Kafka (user-behavior-log)    │
 * │        │                      │
 * │        ▼                      │
 * │  Flink Source (反序列化)        │
 * │        │                      │
 * │   ┌────┼────────┬───────────┐  │
 * │   ▼    ▼        ▼           ▼  │
 * │  PVUV Download Funnel  Anomaly │
 * │   │    │        │        │    │
 * │   ▼    ▼        ▼        ▼    │
 * │  Redis    ClickHouse    日志    │
 * │                               │
 * └───────────────────────────────┘
 *
 * 运行方式：
 *   1. 本地 IDE 直接 Run Main.main()
 *   2. 提交到 Flink 集群：
 *      flink run -c com.analytics.Main target/flink-jobs-1.0.0.jar
 */
public class Main {

    // ======== 可配置参数（生产环境从配置文件读取）========
    private static final String KAFKA_SERVERS = "localhost:9092";
    private static final String KAFKA_TOPIC = "user-behavior-log";
    private static final String CONSUMER_GROUP = "flink-analytics-group";

    private static final String REDIS_HOST = "localhost";
    private static final int REDIS_PORT = 6379;
    private static final String REDIS_PASSWORD = "redis123";

    private static final String CLICKHOUSE_HOST = "localhost";
    private static final int CLICKHOUSE_PORT = 8123;
    private static final String CLICKHOUSE_DB = "analytics";
    private static final String CLICKHOUSE_USER = "admin";
    private static final String CLICKHOUSE_PASSWORD = "admin123";

    public static void main(String[] args) throws Exception {

        // ============================================================
        // Step 0: 创建 Flink 执行环境
        // ============================================================
        // StreamExecutionEnvironment 是 Flink 程序的"根容器"
        // 所有的 Source、Process、Sink 都挂在这个 env 下面
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度（Demo 用 1，生产环境设为 CPU 核数）
        env.setParallelism(1);

        // 开启 Checkpoint（每 60 秒做一次快照，故障恢复用）
        env.enableCheckpointing(60_000);

        System.out.println("========================================");
        System.out.println("  Flink 实时分析作业启动");
        System.out.println("  Kafka: " + KAFKA_SERVERS + " → " + KAFKA_TOPIC);
        System.out.println("  Redis: " + REDIS_HOST + ":" + REDIS_PORT);
        System.out.println("  ClickHouse: " + CLICKHOUSE_HOST + ":" + CLICKHOUSE_PORT);
        System.out.println("========================================");

        // ============================================================
        // Step 1: 从 Kafka 读取数据源
        // ============================================================
        DataStream<UserBehaviorEvent> sourceStream = env
                .fromSource(
                        KafkaEventSource.create(KAFKA_SERVERS, KAFKA_TOPIC, CONSUMER_GROUP),
                        KafkaEventSource.getWatermarkStrategy(),
                        "Kafka-Source"
                )
                .name("Kafka-Source");

        // 打印原始数据到控制台（调试用）
        sourceStream.print("原始事件");

        // ============================================================
        // Step 2: 启动 4 个并行处理作业
        // ============================================================

        // ── 作业 1: 实时 PV/UV → Redis ──
        DataStream<String> pvuvStream = RealtimePVUV.process(sourceStream);
        pvuvStream.addSink(new RedisSink(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD))
                .name("PVUV-Redis-Sink");
        pvuvStream.print("PV/UV");

        // ── 作业 2: 下载量统计 → ClickHouse ──
        DataStream<String> downloadStream = DownloadStats.process(sourceStream);
        downloadStream.addSink(new ClickHouseSink(
                        CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DB,
                        CLICKHOUSE_USER, CLICKHOUSE_PASSWORD))
                .name("Download-ClickHouse-Sink");
        downloadStream.print("下载统计");

        // ── 作业 3: 漏斗分析 → ClickHouse ──
        DataStream<String> funnelStream = FunnelAnalysis.process(sourceStream);
        funnelStream.addSink(new ClickHouseSink(
                        CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DB,
                        CLICKHOUSE_USER, CLICKHOUSE_PASSWORD))
                .name("Funnel-ClickHouse-Sink");
        funnelStream.print("漏斗分析");

        // ── 作业 4: 异常检测 → 控制台（后续可扩展为钉钉/邮件告警）──
        DataStream<String> anomalyStream = AnomalyDetector.process(sourceStream);
        anomalyStream.print("异常告警");

        // ============================================================
        // Step 3: 启动 Flink 作业（阻塞等待）
        // ============================================================
        System.out.println("\n📊 所有作业已提交，开始处理数据...\n");
        env.execute("User Behavior Analytics - Flink Jobs");
    }
}
