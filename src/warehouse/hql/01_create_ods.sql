-- ============================================================
-- ODS 层建表语句（ClickHouse 版）
-- 已在 docker/clickhouse/init-tables.sql 中定义
-- 此文件仅作为文档记录
-- ============================================================

-- ODS (Operational Data Store) = 操作数据存储
-- 职责：原样存储原始日志，不做任何处理
-- 数据来源：Kafka Topic → Flink → ClickHouse
-- 保留时间：30 天

-- 表名: analytics.ods_user_behavior_log
-- 引擎: MergeTree()
-- 分区: 按天 (toYYYYMMDD)
-- 排序键: (event_time, event_type, user_id)
