-- ============================================================
-- ClickHouse 建表SQL
-- 启动时自动执行，创建用户行为分析所需的所有表
-- ============================================================

-- ==================== 1. 原始日志表 (ODS层) ====================
-- 引擎说明：
--   MergeTree = ClickHouse 最核心的引擎，支持按主键排序、按日期分区
--   PARTITION BY toYYYYMMDD(event_time) = 按天分区，查某天数据只扫当天文件
--   ORDER BY (event_time, event_type) = 数据按时间+事件类型物理排序
--   TTL event_time + INTERVAL 30 DAY = 30天后自动删除（节省空间）

CREATE TABLE IF NOT EXISTS analytics.ods_user_behavior_log
(
    -- 事件标识
    event_id        String,              -- 事件唯一ID (UUID)
    event_type      String,              -- 事件类型: page_view/app_click/app_download/...

    -- 用户标识
    user_id         String,              -- 用户ID
    device_id       String,              -- 设备ID（一台设备可能有多个用户）

    -- 时间
    event_time      DateTime,            -- 事件发生时间（精确到秒）
    server_time     DateTime DEFAULT now(), -- 服务端接收时间

    -- 设备信息（面试亮点：手机厂商埋点必带字段）
    device_model    String,              -- 设备型号: 如 Find X6 Pro
    os_version      String,              -- 操作系统版本: 如 Android 14
    app_version     String,              -- App版本号: 如 8.6.3

    -- 会话信息
    session_id      String,              -- 会话ID，串联同一次访问的所有事件

    -- 页面信息
    page            String,              -- 当前页面: home/search/detail/download

    -- 位置信息
    ip              String,              -- IP地址
    city            String,              -- 城市（从IP解析）

    -- 行为数据
    duration        Int32 DEFAULT 0,     -- 页面停留时长(毫秒)

    -- 扩展属性（JSON格式，存放事件特有的字段）
    -- 例如下载事件: {"app_id":"com.xxx","app_name":"抖音","download_source":"search"}
    properties      String DEFAULT '{}',

    -- 数据来源标记
    source          String DEFAULT 'simulator', -- simulator=模拟数据, real=真实数据

    -- AB 实验字段
    experiment_id   String DEFAULT '',         -- 实验ID
    variant         String DEFAULT ''           -- 实验分组: A / B
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (event_time, event_type, user_id)
TTL event_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- 建索引提速：按 event_type 快速过滤
-- ALTER TABLE analytics.ods_user_behavior_log
--     ADD INDEX idx_event_type event_type TYPE set(100) GRANULARITY 5;
-- ↑ 第一次建表时注释掉，后面需要时再执行


-- ==================== 2. 清洗后明细表 (DWD层) ====================
-- 与ODS表结构相同，但数据已经过：
--   1. 去重（同event_id只保留一条）
--   2. 字段校验（user_id不为空、event_type在合法列表中）
--   3. 字段补全（IP→城市映射等）
-- 引擎用 ReplacingMergeTree 实现幂等写入（同event_id重复写入时保留最新一条）

CREATE TABLE IF NOT EXISTS analytics.dwd_user_behavior
(
    event_id        String,
    event_type      String,
    user_id         String,
    device_id       String,
    event_time      DateTime,
    server_time     DateTime,
    device_model    String,
    os_version      String,
    app_version     String,
    session_id      String,
    page            String,
    ip              String,
    city            String,
    duration        Int32 DEFAULT 0,
    properties      String DEFAULT '{}',
    source          String DEFAULT 'simulator',
    experiment_id   String DEFAULT '',
    variant         String DEFAULT ''
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (event_id, event_time)
TTL event_time + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;


-- ==================== 3. 天级汇总表 (DWS层) ====================
-- 预先计算好每天的核心指标，Dashboard查这个表而不是扫原始表
-- 物化视图 或 Spark任务 每天凌晨跑一次写入

CREATE TABLE IF NOT EXISTS analytics.dws_daily_metrics
(
    stat_date       Date,                -- 统计日期
    event_type      String,              -- 事件类型 (all=所有事件汇总)
    pv              UInt64,              -- 页面浏览量
    uv              UInt64,              -- 独立用户数（精确去重）
    new_user_count  UInt64 DEFAULT 0,    -- 新增用户数
    avg_duration    Float64 DEFAULT 0,   -- 平均页面停留时长(ms)
    top_city        String DEFAULT '',   -- 活跃用户最多城市
    top_device      String DEFAULT '',   -- 使用最多的设备型号
    download_count  UInt64 DEFAULT 0,    -- 下载次数
    search_count    UInt64 DEFAULT 0     -- 搜索次数
)
ENGINE = SummingMergeTree()             -- SummingMergeTree: 同主键的行自动合并求和
PARTITION BY toYYYYMM(stat_date)
ORDER BY (stat_date, event_type)
TTL stat_date + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;


-- ==================== 4. 实时指标表 (ADS层/供Dashboard查询) ====================
-- Flink作业每5分钟写入一次
-- 引擎用 MergeTree（不需要去重，Flink保证幂等）

CREATE TABLE IF NOT EXISTS analytics.ads_realtime_metrics
(
    metric_time     DateTime,            -- 指标时间窗口起始
    metric_name     String,              -- 指标名: pv/uv/download_count/search_count
    metric_value    Float64,             -- 指标值
    dimension       String DEFAULT 'all',-- 维度: app_id / city / device_model 或 all
    dimension_value String DEFAULT 'all',-- 维度值: 具体app名 / 城市名 / 机型
    window_size     String DEFAULT '5m'  -- 窗口大小: 5m / 1h / 1d
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(metric_time)
ORDER BY (metric_time, metric_name, dimension, dimension_value)
TTL metric_time + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;


-- ==================== 5. 漏斗分析结果表 ====================
-- 每天跑一次离线漏斗，存结果

CREATE TABLE IF NOT EXISTS analytics.ads_funnel_analysis
(
    stat_date       Date,                -- 统计日期
    funnel_name     String,              -- 漏斗名称: app_download_funnel
    step_order      Int32,              -- 步骤序号: 1=曝光, 2=点击, 3=下载, 4=安装
    step_name       String,              -- 步骤名称: impression/click/download/install
    user_count      UInt64,              -- 该步骤人数
    step_rate       Float64,             -- 该步骤转化率（相对于上一步）
    overall_rate    Float64              -- 整体转化率（相对于第一步）
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(stat_date)
ORDER BY (stat_date, funnel_name, step_order)
TTL stat_date + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;


-- ==================== 6. 留存分析结果表 ====================
-- 每天跑一次离线留存计算，存结果

CREATE TABLE IF NOT EXISTS analytics.ads_retention
(
    first_date      Date,                -- 用户首次访问日期（基准日）
    ret_day         Int32,              -- 第N天留存: 1=次日, 3=3日, 7=7日, 30=30日
    new_user_count  UInt64,              -- 基准日新增用户数
    retained_count  UInt64,              -- 第N天回访用户数
    retention_rate  Float64              -- 留存率 (%)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(first_date)
ORDER BY (first_date, ret_day)
TTL first_date + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;


-- ==================== 7. AB 实验结果表 ====================
-- 每天跑 Spark AB 计算，存在这里供 Dashboard 拉取

CREATE TABLE IF NOT EXISTS analytics.ads_ab_experiment
(
    experiment_id   String,              -- 实验ID
    stat_date       Date,                -- 统计日期
    variant         String,              -- 分组: A 或 B
    metric_name     String,              -- 指标名: pv/uv/download_count/download_conversion_rate/...
    metric_value    Float64,             -- 指标值
    metric_unit     String DEFAULT '',   -- 单位: 次 / % / ms
    user_count      UInt64 DEFAULT 0,    -- 该组用户总数
    p_value         Float64 DEFAULT -1,  -- P值（<0.05 表示统计显著）-1=未计算
    is_significant  UInt8 DEFAULT 0,     -- 是否统计显著: 0=否 1=是
    uplift          Float64 DEFAULT 0,   -- 实验组相对对照组的提升幅度 (%)
    sample_data     String DEFAULT ''    -- 抽样数据(JSON)，用于显著性检验
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(stat_date)
ORDER BY (experiment_id, stat_date, variant, metric_name)
TTL stat_date + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;
