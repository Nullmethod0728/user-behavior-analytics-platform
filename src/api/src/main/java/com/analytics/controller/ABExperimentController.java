package com.analytics.controller;

import com.analytics.service.ClickHouseService;
import com.analytics.vo.Result;
import org.springframework.web.bind.annotation.*;

/**
 * AB 实验数据接口
 *
 * 数据来自 ClickHouse ads_ab_experiment 表（Spark + Python 计算）。
 */
@RestController
@RequestMapping("/api/v1/ab")
public class ABExperimentController {

    private final ClickHouseService clickHouseService;

    public ABExperimentController(ClickHouseService clickHouseService) {
        this.clickHouseService = clickHouseService;
    }

    /**
     * GET /api/v1/ab/experiment?experiment_id=exp_001&date=2026-07-10
     *
     * 查询某实验某日的分组指标对比数据
     */
    @GetMapping("/experiment")
    public Result<?> getExperiment(
            @RequestParam String experimentId,
            @RequestParam(defaultValue = "") String date) {
        if (date.isEmpty()) {
            date = java.time.LocalDate.now().minusDays(1).toString();
        }
        return Result.ok(clickHouseService.getABExperiment(experimentId, date));
    }
}
