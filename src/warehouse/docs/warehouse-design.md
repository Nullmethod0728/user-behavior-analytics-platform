# 📊 数据仓库设计文档

## 1. 分层架构

采用经典的**四层数仓架构**：

```
ODS (操作数据层)
  ↓ 清洗、去重、校验
DWD (明细数据层)
  ↓ 聚合、汇总
DWS (汇总数据层)
  ↓ 业务指标计算
ADS (应用数据层)
```

## 2. 各层说明

| 层级 | 表名 | 引擎 | 更新方式 | 保留时间 |
|------|------|------|---------|---------|
| ODS | ods_user_behavior_log | MergeTree | Flink 实时写入 | 30天 |
| DWD | dwd_user_behavior | ReplacingMergeTree | Spark 每天清洗 | 90天 |
| DWS | dws_daily_metrics | SummingMergeTree | Spark 每天汇总 | 365天 |
| ADS | ads_realtime_metrics | MergeTree | Flink 实时写入 | 7天 |
| ADS | ads_retention | MergeTree | Spark 每天计算 | 365天 |
| ADS | ads_funnel_analysis | MergeTree | Spark 每天计算 | 365天 |
| ADS | ads_user_profile | MergeTree | Spark 全量更新 | 长期 |

## 3. 数据流向

```
Python 生成器 → Kafka → Flink → ODS 表
                              → ADS(实时指标) → Redis → Dashboard

                    Spark(每天) → DWD → DWS
                                     → ADS(留存/漏斗/画像) → ClickHouse → BI
```

## 4. SQL 范例

### 查询某天的 PV/UV
```sql
SELECT stat_date, pv, uv
FROM analytics.dws_daily_metrics
WHERE stat_date >= '2026-07-01'
ORDER BY stat_date;
```

### 查询下载量 Top 10 的 App
```sql
SELECT
    JSONExtractString(properties, 'app_name') AS app_name,
    count() AS download_count
FROM analytics.dwd_user_behavior
WHERE event_type = 'app_download'
  AND toDate(event_time) = '2026-07-10'
GROUP BY app_name
ORDER BY download_count DESC
LIMIT 10;
```

### 查询某日漏斗转化
```sql
SELECT step_order, step_name, user_count,
       step_rate AS '步骤转化率(%)',
       overall_rate AS '总转化率(%)'
FROM analytics.ads_funnel_analysis
WHERE stat_date = '2026-07-10'
ORDER BY step_order;
```
