package com.analytics.controller;

import com.analytics.service.ClickHouseService;
import com.analytics.vo.Result;
import org.springframework.web.bind.annotation.*;

/**
 * 留存与指标数据接口
 *
 * 数据来自 ClickHouse（Spark 每天凌晨计算）。
 */
@RestController
@RequestMapping("/api/v1/analytics")
public class RetentionController {

    private final ClickHouseService clickHouseService;

    public RetentionController(ClickHouseService clickHouseService) {
        this.clickHouseService = clickHouseService;
    }

    /**
     * GET /api/v1/analytics/retention?date=2026-07-10
     *
     * 查询某日的留存率
     */
    @GetMapping("/retention")
    public Result<?> getRetention(
            @RequestParam(defaultValue = "") String date) {
        if (date.isEmpty()) {
            date = java.time.LocalDate.now().minusDays(1).toString();
        }
        return Result.ok(clickHouseService.getRetention(date));
    }

    /**
     * GET /api/v1/analytics/daily?from=2026-07-01&to=2026-07-10
     *
     * 查询日期范围内的天级汇总指标
     */
    @GetMapping("/daily")
    public Result<?> getDailyMetrics(
            @RequestParam String from,
            @RequestParam String to) {
        return Result.ok(clickHouseService.getDailyMetrics(from, to));
    }

    /**
     * GET /api/v1/analytics/top-downloads?limit=10
     *
     * 查询最近 7 天下载量排行
     */
    @GetMapping("/top-downloads")
    public Result<?> getTopDownloads(
            @RequestParam(defaultValue = "10") int limit) {
        return Result.ok(clickHouseService.getTopDownloads(limit));
    }
}
