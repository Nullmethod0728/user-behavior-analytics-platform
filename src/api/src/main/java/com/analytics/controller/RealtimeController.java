package com.analytics.controller;

import com.analytics.service.RedisService;
import com.analytics.vo.Result;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 实时数据接口
 *
 * Dashboard 实时大屏的翻牌器和趋势图走这个接口。
 * 数据来自 Redis（Flink 写入），毫秒级返回。
 */
@RestController
@RequestMapping("/api/v1/realtime")
public class RealtimeController {

    private final RedisService redisService;

    public RealtimeController(RedisService redisService) {
        this.redisService = redisService;
    }

    /**
     * GET /api/v1/realtime/metrics
     *
     * 获取所有实时指标，Dashboard 每 3-5 秒轮询一次
     */
    @GetMapping("/metrics")
    public Result<?> getMetrics() {
        return Result.ok(redisService.getRealtimeMetrics());
    }
}
