-- ============================================================
-- 经典留存查询 SQL（面试高频！）
-- ============================================================

-- 查询某日的次日/3日/7日/30日留存率
SELECT
    first_date,
    ret_day,
    new_user_count,
    retained_count,
    retention_rate
FROM analytics.ads_retention
WHERE first_date = '2026-07-10'
ORDER BY ret_day;

-- 输出示例:
-- first_date  | ret_day | new_user_count | retained_count | retention_rate
-- 2026-07-10  | 1       | 5000           | 2000           | 40.00
-- 2026-07-10  | 3       | 5000           | 1250           | 25.00
-- 2026-07-10  | 7       | 5000           | 750            | 15.00
-- 2026-07-10  | 30      | 5000           | 400            | 8.00
