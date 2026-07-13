package com.analytics.service;

import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;

/**
 * Redis 查询服务
 *
 * 负责读取实时指标：当前 PV、UV、下载量
 * Dashboard 实时大屏的数字翻牌器每秒轮询这个接口。
 */
@Service
public class RedisService {

    private final JedisPool jedisPool;

    public RedisService(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    /**
     * 获取实时指标
     *
     * Redis Key: analytics:realtime:metrics
     * Value: JSON {"metric":"pv_uv","pv":128000,"uv":45000,...}
     */
    public Map<String, Object> getRealtimeMetrics() {
        Map<String, Object> result = new HashMap<>();

        try (Jedis jedis = jedisPool.getResource()) {
            // 获取最新的实时指标
            String metricsJson = jedis.get("analytics:realtime:metrics");
            if (metricsJson != null) {
                result.put("raw", metricsJson);
            } else {
                // 没有数据时返回默认值
                result.put("raw", "{\"pv\":0,\"uv\":0}");
            }

            // 也可以单独获取各个指标
            String currentPv = jedis.get("analytics:realtime:pv");
            String currentUv = jedis.get("analytics:realtime:uv");

            result.put("pv", currentPv != null ? Long.parseLong(currentPv) : 0);
            result.put("uv", currentUv != null ? Long.parseLong(currentUv) : 0);

            // Redis 连接状态
            result.put("redis_connected", true);

        } catch (Exception e) {
            result.put("redis_connected", false);
            result.put("error", e.getMessage());
        }

        return result;
    }

    /**
     * 健康检查：Redis 是否可用
     */
    public boolean isConnected() {
        try (Jedis jedis = jedisPool.getResource()) {
            return "PONG".equals(jedis.ping());
        } catch (Exception e) {
            return false;
        }
    }
}
