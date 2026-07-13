package com.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * 用户行为分析平台 — Spring Boot 入口
 *
 * 启动后访问:
 *   http://localhost:8080/actuator/health            — 健康检查
 *   http://localhost:8080/api/v1/realtime/metrics    — 实时指标
 *   http://localhost:8080/api/v1/analytics/retention?date=2026-07-10  — 留存
 *   http://localhost:8080/api/v1/analytics/funnel?date=2026-07-10    — 漏斗
 *   http://localhost:8080/api/v1/analytics/daily?from=2026-07-01&to=2026-07-10
 *   http://localhost:8080/api/v1/analytics/top-downloads?limit=10
 */
@SpringBootApplication
public class AnalyticsApplication {

    public static void main(String[] args) {
        SpringApplication.run(AnalyticsApplication.class, args);
    }

    /**
     * 跨域配置 — 允许 Dashboard 前端访问 API
     *
     * 前端在 localhost:3000（或直接用 file:// 打开 HTML），
     * API 在 localhost:8080，浏览器默认禁止跨域请求。
     * 这里配置放开。
     */
    @Bean
    public WebMvcConfigurer corsConfigurer() {
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/api/**")         // 所有 API 路径
                        .allowedOrigins("*")            // 允许任何来源（开发用）
                        .allowedMethods("GET", "POST")  // 允许的 HTTP 方法
                        .maxAge(3600);                  // 预检请求缓存 1 小时
            }
        };
    }
}
