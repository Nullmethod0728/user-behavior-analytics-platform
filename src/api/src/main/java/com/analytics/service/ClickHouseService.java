package com.analytics.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * ClickHouse 查询服务
 *
 * 负责查询历史指标：留存率、漏斗数据、天级汇总
 */
@Service
public class ClickHouseService {

    private final JdbcTemplate jdbcTemplate;

    public ClickHouseService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * 查询指定日期的留存率
     */
    public List<Map<String, Object>> getRetention(String date) {
        String sql = """
            SELECT
                first_date,
                ret_day,
                new_user_count,
                retained_count,
                retention_rate
            FROM analytics.ads_retention
            WHERE first_date = ?
            ORDER BY ret_day
            """;
        return jdbcTemplate.queryForList(sql, date);
    }

    /**
     * 查询指定日期的漏斗数据
     */
    public List<Map<String, Object>> getFunnel(String date) {
        String sql = """
            SELECT
                step_order,
                step_name,
                user_count,
                step_rate,
                overall_rate
            FROM analytics.ads_funnel_analysis
            WHERE stat_date = ?
            ORDER BY step_order
            """;
        return jdbcTemplate.queryForList(sql, date);
    }

    /**
     * 查询天级汇总指标（支持日期范围）
     */
    public List<Map<String, Object>> getDailyMetrics(String fromDate, String toDate) {
        String sql = """
            SELECT
                stat_date,
                event_type,
                pv,
                uv,
                new_user_count,
                avg_duration,
                top_city,
                top_device,
                download_count,
                search_count
            FROM analytics.dws_daily_metrics
            WHERE stat_date BETWEEN ? AND ?
            ORDER BY stat_date
            """;
        return jdbcTemplate.queryForList(sql, fromDate, toDate);
    }

    /**
     * 下载量 Top N 排行（最近 7 天）
     */
    public List<Map<String, Object>> getTopDownloads(int limit) {
        String sql = """
            SELECT
                JSONExtractString(properties, 'app_name') AS app_name,
                count() AS download_count
            FROM analytics.dwd_user_behavior
            WHERE event_type = 'app_download'
              AND toDate(event_time) >= today() - 7
            GROUP BY app_name
            ORDER BY download_count DESC
            LIMIT ?
            """;
        return jdbcTemplate.queryForList(sql, limit);
    }
}
