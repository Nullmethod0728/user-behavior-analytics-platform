-- ============================================================
-- Hive 兼容性对照文档
-- 用途：将项目中的 ClickHouse SQL 翻译为 Hive QL 等价写法
-- 面试说明：项目使用 Spark SQL 进行计算（语法兼容 Hive SQL），
--           此处提供 Hive 版本 DDL/DML 作为对照
-- ============================================================

-- ============================================================
-- 第一部分：Hive vs ClickHouse 核心差异速览
-- ============================================================

-- | 维度         | ClickHouse                    | Hive                          |
-- |-------------|-------------------------------|-------------------------------|
-- | 存储格式      | 自有列式格式（MergeTree 引擎族）  | ORC / Parquet（建表时指定）     |
-- | 计算引擎      | 自有向量化引擎                  | MapReduce / Tez / Spark       |
-- | 分区机制      | PARTITION BY toYYYYMMDD(col)  | PARTITIONED BY (dt STRING)    |
-- | 更新能力      | ReplacingMergeTree 去重       | 不支持 UPDATE，全量覆盖写入     |
-- | 自动聚合      | SummingMergeTree 自动合并      | 需手动 GROUP BY 聚合           |
-- | TTL 自动过期  | TTL event_time + INTERVAL 30  | 需定时任务 ALTER TABLE DROP   |
-- | JSON 解析     | JSONExtractString()           | get_json_object()             |
-- | 时间函数      | toDate(event_time)            | to_date(event_time)           |


-- ============================================================
-- 第二部分：建表语句对照（ClickHouse → Hive）
-- ============================================================

-- ---------------------------
-- 2.1 ODS 层 - 原始日志表
-- ---------------------------

-- [ClickHouse 原版]
-- CREATE TABLE analytics.ods_user_behavior_log (
--     event_id String, event_type String, user_id String, ...
-- ) ENGINE = MergeTree()
-- PARTITION BY toYYYYMMDD(event_time)
-- ORDER BY (event_time, event_type, user_id)
-- TTL event_time + INTERVAL 30 DAY;

-- [Hive 等价写法]
CREATE TABLE IF NOT EXISTS analytics.ods_user_behavior_log (
    event_id        STRING          COMMENT '事件唯一ID',
    event_type      STRING          COMMENT '事件类型: page_view/app_click/app_download/...',
    user_id         STRING          COMMENT '用户ID',
    device_id       STRING          COMMENT '设备ID',
    event_time      TIMESTAMP       COMMENT '事件发生时间',
    server_time     TIMESTAMP       COMMENT '服务端接收时间',
    device_model    STRING          COMMENT '设备型号',
    os_version      STRING          COMMENT '操作系统版本',
    app_version     STRING          COMMENT 'App版本号',
    session_id      STRING          COMMENT '会话ID',
    page            STRING          COMMENT '当前页面',
    ip              STRING          COMMENT 'IP地址',
    city            STRING          COMMENT '城市',
    duration        INT             COMMENT '页面停留时长(毫秒)',
    properties      STRING          COMMENT '扩展属性(JSON格式)',
    source          STRING          COMMENT '数据来源标记'
)
COMMENT '用户行为原始日志表（ODS层）'
PARTITIONED BY (dt STRING COMMENT '分区字段：日期，格式yyyy-MM-dd')
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'orc.compress' = 'SNAPPY'
);

-- 面试要点：Hive 用 PARTITIONED BY 声明分区字段（dt 不存表数据里），
-- ClickHouse 用 PARTITION BY 表达式从已有字段派生。ClickHouse 的分区
-- 更灵活（支持表达式），Hive 的分区更显式。


-- ---------------------------
-- 2.2 DWD 层 - 清洗后明细表
-- ---------------------------

-- [ClickHouse 原版]
-- ENGINE = ReplacingMergeTree()
-- ORDER BY (event_id, event_time)
-- TTL event_time + INTERVAL 90 DAY;

-- [Hive 等价写法]
CREATE TABLE IF NOT EXISTS analytics.dwd_user_behavior (
    event_id        STRING          COMMENT '事件唯一ID（业务主键，用于去重）',
    event_type      STRING          COMMENT '事件类型',
    user_id         STRING          COMMENT '用户ID',
    device_id       STRING          COMMENT '设备ID',
    event_time      TIMESTAMP       COMMENT '事件发生时间',
    server_time     TIMESTAMP       COMMENT '服务端接收时间',
    device_model    STRING          COMMENT '设备型号',
    os_version      STRING          COMMENT '操作系统版本',
    app_version     STRING          COMMENT 'App版本号',
    session_id      STRING          COMMENT '会话ID',
    page            STRING          COMMENT '当前页面',
    ip              STRING          COMMENT 'IP地址',
    city            STRING          COMMENT '城市',
    duration        INT             COMMENT '页面停留时长(毫秒)',
    properties      STRING          COMMENT '扩展属性(JSON格式)',
    source          STRING          COMMENT '数据来源标记'
)
COMMENT '清洗后用户行为明细表（DWD层）'
PARTITIONED BY (dt STRING COMMENT '分区字段：日期，格式yyyy-MM-dd')
CLUSTERED BY (event_id) INTO 32 BUCKETS      -- Hive 特有：分桶，加速 JOIN 和去重
STORED AS ORC                                 -- ORC 格式：压缩率更高、支持 predicate pushdown
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.bloom.filter.columns' = 'event_id'   -- 布隆过滤器：快速判断 event_id 是否存在
);

-- 面试要点：ClickHouse 用 ReplacingMergeTree 在 merge 时自动去重（异步），
-- Hive 需要手动处理去重——ETL 时用 row_number() OVER (PARTITION BY event_id ORDER BY server_time DESC)
-- 取 rn=1 的记录写入 DWD。这就是 DWD 层清洗的核心逻辑之一。


-- ---------------------------
-- 2.3 DWS 层 - 天级汇总表
-- ---------------------------

-- [ClickHouse 原版]
-- ENGINE = SummingMergeTree()
-- ORDER BY (stat_date, event_type)
-- TTL stat_date + INTERVAL 365 DAY;

-- [Hive 等价写法]
CREATE TABLE IF NOT EXISTS analytics.dws_daily_metrics (
    stat_date       DATE            COMMENT '统计日期',
    event_type      STRING          COMMENT '事件类型（all=所有事件汇总）',
    pv              BIGINT          COMMENT '页面浏览量',
    uv              BIGINT          COMMENT '独立用户数（精确去重）',
    new_user_count  BIGINT          COMMENT '新增用户数',
    avg_duration    DOUBLE          COMMENT '平均页面停留时长(ms)',
    top_city        STRING          COMMENT '活跃用户最多城市',
    top_device      STRING          COMMENT '使用最多的设备型号',
    download_count  BIGINT          COMMENT '下载次数',
    search_count    BIGINT          COMMENT '搜索次数'
)
COMMENT '天级汇总指标表（DWS层）'
PARTITIONED BY (month STRING COMMENT '分区字段：月份，格式yyyy-MM')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 面试要点：Hive 不区分 MergeTree/SummingMergeTree，所有表本质都是 HDFS 上的文件。
-- SummingMergeTree 的自动求和能力在 Hive 中需要用 INSERT OVERWRITE + GROUP BY 手动实现，
-- 即每次写入 DWS 表时对 stat_date + event_type 做聚合。


-- ---------------------------
-- 2.4 ADS 层 - 漏斗分析结果表
-- ---------------------------

CREATE TABLE IF NOT EXISTS analytics.ads_funnel_analysis (
    stat_date       DATE            COMMENT '统计日期',
    funnel_name     STRING          COMMENT '漏斗名称',
    step_order      INT             COMMENT '步骤序号',
    step_name       STRING          COMMENT '步骤名称',
    user_count      BIGINT          COMMENT '该步骤人数',
    step_rate       DOUBLE          COMMENT '步骤转化率(%)',
    overall_rate    DOUBLE          COMMENT '整体转化率(%)'
)
COMMENT '漏斗分析结果表（ADS层）'
PARTITIONED BY (month STRING COMMENT '分区字段：月份')
STORED AS ORC;


-- ---------------------------
-- 2.5 ADS 层 - 留存分析结果表
-- ---------------------------

CREATE TABLE IF NOT EXISTS analytics.ads_retention (
    first_date      DATE            COMMENT '用户首次访问日期（基准日）',
    ret_day         INT             COMMENT '第N天留存',
    new_user_count  BIGINT          COMMENT '基准日新增用户数',
    retained_count  BIGINT          COMMENT '第N天回访用户数',
    retention_rate  DOUBLE          COMMENT '留存率(%)'
)
COMMENT '留存分析结果表（ADS层）'
PARTITIONED BY (month STRING COMMENT '分区字段：月份')
STORED AS ORC;


-- ============================================================
-- 第三部分：核心查询对照（ClickHouse → Hive QL）
-- ============================================================

-- ---------------------------
-- 3.1 查询某天 PV/UV
-- ---------------------------

-- [ClickHouse]
-- SELECT stat_date, pv, uv
-- FROM analytics.dws_daily_metrics
-- WHERE stat_date >= '2026-07-01'
-- ORDER BY stat_date;

-- [Hive QL — 几乎一样]
SELECT stat_date, pv, uv
FROM analytics.dws_daily_metrics
WHERE stat_date >= '2026-07-01'
ORDER BY stat_date;

-- 两者语法完全相同（都遵循 ANSI SQL）


-- ---------------------------
-- 3.2 查询下载量 Top 10
-- ---------------------------

-- [ClickHouse]
-- SELECT
--     JSONExtractString(properties, 'app_name') AS app_name,
--     count() AS download_count
-- FROM analytics.dwd_user_behavior
-- WHERE event_type = 'app_download'
--   AND toDate(event_time) = '2026-07-10'
-- GROUP BY app_name
-- ORDER BY download_count DESC
-- LIMIT 10;

-- [Hive QL — 函数名不同]
SELECT
    get_json_object(properties, '$.app_name') AS app_name,
    COUNT(*) AS download_count
FROM analytics.dwd_user_behavior
WHERE event_type = 'app_download'
  AND dt = '2026-07-10'                          -- Hive 用 dt 分区字段过滤
  AND to_date(event_time) = '2026-07-10'         -- 额外精确过滤
GROUP BY get_json_object(properties, '$.app_name')
ORDER BY download_count DESC
LIMIT 10;

-- 差异点：
--   ① JSONExtractString() → get_json_object()
--   ② toDate() → to_date()
--   ③ count() → COUNT(*)
--   ④ Hive 多了 dt 分区裁剪（核心性能优化手段）


-- ---------------------------
-- 3.3 留存率计算（面试核心 SQL）
-- ---------------------------

-- [Hive QL 版本 — 次日留存率]
WITH
-- Step 1: 基准日活跃用户
base_users AS (
    SELECT DISTINCT user_id
    FROM analytics.dwd_user_behavior
    WHERE dt = '2026-07-10'
      AND user_id IS NOT NULL
),
-- Step 2: 次日在访用户
return_users AS (
    SELECT DISTINCT user_id
    FROM analytics.dwd_user_behavior
    WHERE dt = '2026-07-11'
      AND user_id IS NOT NULL
)
-- Step 3: LEFT JOIN 算交集
SELECT
    '2026-07-10'                    AS first_date,
    1                               AS ret_day,
    COUNT(DISTINCT b.user_id)       AS new_user_count,
    COUNT(DISTINCT r.user_id)       AS retained_count,
    ROUND(COUNT(DISTINCT r.user_id) * 100.0
          / COUNT(DISTINCT b.user_id), 2) AS retention_rate
FROM base_users b
LEFT JOIN return_users r ON b.user_id = r.user_id;

-- 面试要点：
--   ① CTE (WITH AS) 把逻辑拆成两步，可读性更好
--   ② LEFT JOIN 保证基准用户全部保留，次日没回来的 r.user_id 为 NULL
--   ③ 逐日 UNION ALL 可以一次性算出次日/3日/7日/30日
--   ④ Hive 中 COUNT(DISTINCT) 会触发 Reduce 端全排序，数据量大时可改用
--      size(collect_set(user_id)) 近似替代


-- ---------------------------
-- 3.4 批量计算次日/3日/7日/30日留存
-- ---------------------------

WITH base AS (
    SELECT DISTINCT user_id, '2026-07-10' AS base_date
    FROM analytics.dwd_user_behavior
    WHERE dt = '2026-07-10' AND user_id IS NOT NULL
)
SELECT
    b.base_date AS first_date,
    d.ret_day,
    COUNT(DISTINCT b.user_id) AS new_user_count,
    COUNT(DISTINCT r.user_id) AS retained_count,
    ROUND(COUNT(DISTINCT r.user_id) * 100.0
          / COUNT(DISTINCT b.user_id), 2) AS retention_rate
FROM base b
CROSS JOIN (
    SELECT 1 AS ret_day, '2026-07-11' AS ret_date
    UNION ALL SELECT 3,  '2026-07-13'
    UNION ALL SELECT 7,  '2026-07-17'
    UNION ALL SELECT 30, '2026-08-09'
) d
LEFT JOIN analytics.dwd_user_behavior r
    ON b.user_id = r.user_id
    AND r.dt = d.ret_date
GROUP BY b.base_date, d.ret_day
ORDER BY d.ret_day;

-- 技巧：用 CROSS JOIN + 日期枚举表替代多次 JOIN，一次性产出 4 个留存值


-- ---------------------------
-- 3.5 漏斗分析（用户级别精确漏斗）
-- ---------------------------

-- [Hive QL]
WITH funnel_steps AS (
    SELECT
        user_id,
        -- 每个用户在每个步骤是否有行为（1=有，0=无）
        MAX(CASE WHEN event_type = 'page_view'    THEN 1 ELSE 0 END) AS step1_impression,
        MAX(CASE WHEN event_type = 'app_click'    THEN 1 ELSE 0 END) AS step2_click,
        MAX(CASE WHEN event_type = 'app_download' THEN 1 ELSE 0 END) AS step3_download,
        MAX(CASE WHEN event_type = 'app_install'  THEN 1 ELSE 0 END) AS step4_install
    FROM analytics.dwd_user_behavior
    WHERE dt = '2026-07-10'
    GROUP BY user_id
)
SELECT
    '2026-07-10'            AS stat_date,
    'app_download_funnel'   AS funnel_name,
    1                       AS step_order,
    'impression'            AS step_name,
    COUNT(DISTINCT user_id) AS user_count                   -- 曝光人数 = 所有用户
FROM funnel_steps
UNION ALL
SELECT '2026-07-10', 'app_download_funnel', 2, 'click',
       COUNT(DISTINCT CASE WHEN step1_impression = 1 AND step2_click = 1 THEN user_id END)
FROM funnel_steps
UNION ALL
SELECT '2026-07-10', 'app_download_funnel', 3, 'download',
       COUNT(DISTINCT CASE WHEN step1_impression = 1 AND step2_click = 1
                              AND step3_download = 1 THEN user_id END)
FROM funnel_steps
UNION ALL
SELECT '2026-07-10', 'app_download_funnel', 4, 'install',
       COUNT(DISTINCT CASE WHEN step1_impression = 1 AND step2_click = 1
                              AND step3_download = 1 AND step4_install = 1 THEN user_id END)
FROM funnel_steps;

-- 面试要点：漏斗"缩紧"逻辑——每个步骤的用户必须是完成前面所有步骤的用户，
-- 这样每一步的人数才是有序递减的。


-- ============================================================
-- 第四部分：Hive 面试高频概念速查
-- ============================================================

-- 4.1 分区表 vs 分桶表
-- 分区（PARTITIONED BY）：按字段值把数据分到不同 HDFS 目录
--   例：dt=2026-07-10/ → 只扫这一个目录，跳过其他所有日期的数据
--   适用场景：日期、地区等粗粒度字段
-- 分桶（CLUSTERED BY）：按字段 hash 把同一分区的数据再分到 N 个文件
--   例：按 user_id 分 32 桶 → JOIN 时两个表按相同字段分桶，可以避免 Shuffle
--   适用场景：高频 JOIN 字段、采样查询（TABLESAMPLE）

-- 4.2 ORC vs Parquet
-- |          | ORC                     | Parquet               |
-- |---------|--------------------------|-----------------------|
-- | 压缩率    | 更高（针对 Hive 优化）      | 稍低                    |
-- | 查询性能  | 更好（predicate pushdown）  | 略逊                    |
-- | 生态      | Hive 专属                 | 通用（Spark/Presto/...  |
-- | 推荐      | Hive 原生场景用 ORC        | 多引擎兼容用 Parquet     |

-- 4.3 Spark SQL 为什么兼容 Hive SQL
-- Spark 提供了 HiveContext，可以连接 Hive Metastore 读取表的元数据（Schema、分区、存储格式）。
-- 大部分 Hive QL 语法（SELECT/JOIN/CTE/窗口函数/INSERT OVERWRITE）Spark SQL 都可以执行。
-- 区别在于执行引擎：Hive 默认走 MapReduce，Spark 走内存 DAG 计算，后者通常快 10-100 倍。
-- 这就是为什么很多公司用 Hive Metastore 存元数据 + Spark 做计算引擎的组合方案。

-- 4.4 INSERT OVERWRITE vs INSERT INTO
-- INSERT OVERWRITE: 覆盖写入（替换整个分区）—— Hive 默认，ETL 最常用
-- INSERT INTO: 追加写入 —— 需要配置事务表（ACID）才支持
-- 项目中的 Spark OdsToDwd 作业本质上就是 INSERT OVERWRITE 到 DWD 表的 dt 分区

-- 4.5 动态分区（Dynamic Partition）
-- 写入时根据数据内容自动创建分区，避免手动枚举每个分区值：
-- SET hive.exec.dynamic.partition = true;
-- SET hive.exec.dynamic.partition.mode = nonstrict;
-- INSERT OVERWRITE TABLE analytics.dwd_user_behavior PARTITION(dt)
-- SELECT ..., to_date(event_time) AS dt FROM ...;
