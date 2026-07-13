package com.analytics.etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * ODS → DWD 数据清洗
 *
 * ┌──────── 做了什么 ────────┐
 * │ 1. 去重：同一 event_id 只保留一条    │
 * │ 2. 校验：user_id 不为空，event_type 在合法列表 │
 * │ 3. 补全：空字段用默认值填充           │
 * │ 4. 标准化：event_time 转成时间戳        │
 * └──────────────────────────┘
 *
 * 类比：菜市场买回来的菜 → 洗干净、切好、去掉烂叶子
 */
object OdsToDwd {

  /** 合法的 event_type 列表 */
  private val VALID_EVENT_TYPES = Set(
    "page_view", "app_click", "search_query",
    "app_download", "ad_impression", "ad_click",
    "app_install", "in_app_purchase"
  )

  /**
   * 执行清洗
   *
   * @param spark   SparkSession
   * @param odsData 从 ClickHouse ODS 表读进来的 DataFrame
   * @return 清洗后的 DataFrame（可直接写入 DWD 表）
   */
  def execute(spark: SparkSession, odsData: DataFrame): DataFrame = {
    import spark.implicits._

    println("=" * 50)
    println("[ODS→DWD] 开始数据清洗...")
    println(s"[ODS→DWD] 输入行数: ${odsData.count()}")

    // Step 1: 去重 — 按 event_id 去重，保留最新一条
    val deduped = odsData
      .dropDuplicates("event_id")
    println(s"[ODS→DWD] 去重后: ${deduped.count()}")

    // Step 2: 数据校验
    //   - user_id 不能为空
    //   - event_type 必须在合法列表中
    val validated = deduped
      .filter(col("user_id").isNotNull && col("user_id") =!= "")
      .filter(col("user_id") =!= "null")
      .filter(col("event_type").isin(VALID_EVENT_TYPES.toSeq: _*))
    println(s"[ODS→DWD] 校验后: ${validated.count()}")

    // Step 3: 填充空字段
    val cleaned = validated
      .withColumn("city",
        when(col("city").isNull || col("city") === "", lit("未知"))
          .otherwise(col("city")))
      .withColumn("device_model",
        when(col("device_model").isNull || col("device_model") === "", lit("未知"))
          .otherwise(col("device_model")))
      .withColumn("duration",
        when(col("duration").isNull, lit(0))
          .otherwise(col("duration")))

    // Step 4: 标准化时间列（确保是 Timestamp 类型）
    val result = cleaned
      .withColumn("event_time", to_timestamp(col("event_time")))
      .withColumn("server_time", to_timestamp(col("server_time")))

    println(s"[ODS→DWD] 输出行数: ${result.count()}")
    println("=" * 50)

    result
  }
}
