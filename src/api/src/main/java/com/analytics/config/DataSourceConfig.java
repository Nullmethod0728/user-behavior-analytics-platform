package com.analytics.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.sql.DataSource;
import java.time.Duration;

/**
 * 数据源配置 — ClickHouse + Redis
 *
 * 不使用 Spring Boot 的自动配置（因为它默认配的是 MySQL/H2），
 * 而是手动创建 ClickHouse 和 Redis 的连接池 Bean。
 */
@Configuration
public class DataSourceConfig {

    // ── ClickHouse 配置 ──
    @Value("${clickhouse.url}")
    private String clickhouseUrl;

    @Value("${clickhouse.user}")
    private String clickhouseUser;

    @Value("${clickhouse.password}")
    private String clickhousePassword;

    // ── Redis 配置 ──
    @Value("${spring.data.redis.host}")
    private String redisHost;

    @Value("${spring.data.redis.port}")
    private int redisPort;

    @Value("${spring.data.redis.password}")
    private String redisPassword;

    // ============================================================
    // ClickHouse DataSource + JdbcTemplate
    // ============================================================

    @Bean
    public DataSource clickhouseDataSource() {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("com.clickhouse.jdbc.ClickHouseDriver");
        ds.setUrl(clickhouseUrl);
        ds.setUsername(clickhouseUser);
        ds.setPassword(clickhousePassword);
        return ds;
    }

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    // ============================================================
    // Redis JedisPool
    // ============================================================
    // 直接使用 Jedis（比 Spring Data Redis 更底层，面试展示你对 Redis 客户端的理解）

    @Bean
    public JedisPool jedisPool() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(20);
        config.setMaxIdle(10);
        config.setMinIdle(5);
        config.setMaxWait(Duration.ofSeconds(3));

        return new JedisPool(config, redisHost, redisPort, 2000, redisPassword);
    }
}
