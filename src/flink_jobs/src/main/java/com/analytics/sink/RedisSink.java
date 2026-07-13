package com.analytics.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

/**
 * Redis Sink — 实时指标写入 Redis
 *
 * 把 Flink 算出来的实时指标写入 Redis，
 * Dashboard 通过 Spring Boot API 从 Redis 读取，毫秒级刷新。
 *
 * Redis 数据结构设计：
 *   Key:   analytics:realtime:pv        → 值: "128000"
 *   Key:   analytics:realtime:uv        → 值: "45000"
 *   Key:   analytics:realtime:download  → 值: "3200"
 *
 * 带过期时间：60 秒后自动删除（因为 Dashboard 只关心最新值，
 * 旧数据过期后自动清理，不占内存）
 */
public class RedisSink extends RichSinkFunction<String> {

    private transient JedisPool jedisPool;

    private final String redisHost;
    private final int redisPort;
    private final String redisPassword;

    public RedisSink(String host, int port, String password) {
        this.redisHost = host;
        this.redisPort = port;
        this.redisPassword = password;
    }

    @Override
    public void open(Configuration parameters) {
        // 初始化 Redis 连接池
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(10);                          // 最大连接数
        config.setMaxIdle(5);                            // 最大空闲连接
        config.setMinIdle(2);                            // 最小空闲连接
        config.setMaxWait(Duration.ofSeconds(3));        // 获取连接最大等待时间

        jedisPool = new JedisPool(config, redisHost, redisPort,
                2000,   // 连接超时(ms)
                redisPassword);
    }

    @Override
    public void invoke(String value, Context context) {
        // value 是 JSON 字符串: {"metric":"pv_uv","pv":128000,"uv":45000,...}
        try (Jedis jedis = jedisPool.getResource()) {

            // 用当前时间戳做 Key（Dashboard 只取最新的）
            String key = "analytics:realtime:metrics";
            jedis.set(key, value);

            // 60 秒后过期
            jedis.expire(key, 60);

        } catch (Exception e) {
            System.err.println("[RedisSink] 写入 Redis 失败: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }
}
