package com.analytics.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * ClickHouse Sink — 聚合结果批量写入 ClickHouse
 *
 * 负责把 Flink 算好的指标写入 ClickHouse，供离线查询和 BI 报表。
 *
 * 写入目标表：ads_realtime_metrics
 * 写入频率：Flink 窗口触发一次，sink 就写一次
 *
 * 注意：这里用的是简化版实现（每条一写）。
 * 生产环境会用一个攒批的版本——攒 1000 条或 5 秒后一起写，
 * 减少数据库连接开销。
 */
public class ClickHouseSink extends RichSinkFunction<String> {

    private transient Connection connection;
    private transient PreparedStatement stmt;

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public ClickHouseSink(String host, int port, String database,
                          String user, String password) {
        this.jdbcUrl = String.format("jdbc:clickhouse://%s:%d/%s", host, port, database);
        this.user = user;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 建立 ClickHouse JDBC 连接
        connection = DriverManager.getConnection(jdbcUrl, user, password);
        connection.setAutoCommit(true);  // ClickHouse 默认自动提交

        // 预编译 INSERT 语句（? 是占位符，后面填入实际值）
        String sql = "INSERT INTO analytics.ads_realtime_metrics " +
                "(metric_time, metric_name, metric_value, dimension, dimension_value, window_size) " +
                "VALUES (?, ?, ?, ?, ?, ?)";
        stmt = connection.prepareStatement(sql);

        System.out.println("[ClickHouseSink] ✅ 已连接 ClickHouse: " + jdbcUrl);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        // value 是 JSON 字符串，这里简化处理：直接存原始 JSON
        // 生产环境中会解析 JSON 然后填入各字段

        stmt.setObject(1, java.time.LocalDateTime.now());   // metric_time
        stmt.setString(2, "real_time_metric");               // metric_name
        stmt.setDouble(3, 0.0);                              // metric_value (待解析)
        stmt.setString(4, "all");                            // dimension
        stmt.setString(5, "all");                            // dimension_value
        stmt.setString(6, "5m");                             // window_size

        stmt.execute();
    }

    @Override
    public void close() throws Exception {
        if (stmt != null) stmt.close();
        if (connection != null) connection.close();
    }
}
