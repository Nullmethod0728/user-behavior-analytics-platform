package com.analytics.controller;

import com.analytics.service.ClickHouseService;
import com.analytics.vo.Result;
import org.springframework.web.bind.annotation.*;

/**
 * 漏斗分析接口
 */
@RestController
@RequestMapping("/api/v1/analytics")
public class FunnelController {

    private final ClickHouseService clickHouseService;

    public FunnelController(ClickHouseService clickHouseService) {
        this.clickHouseService = clickHouseService;
    }

    /**
     * GET /api/v1/analytics/funnel?date=2026-07-10
     *
     * 查询某日的漏斗转化数据
     */
    @GetMapping("/funnel")
    public Result<?> getFunnel(
            @RequestParam(defaultValue = "") String date) {
        if (date.isEmpty()) {
            date = java.time.LocalDate.now().minusDays(1).toString();
        }
        return Result.ok(clickHouseService.getFunnel(date));
    }
}
