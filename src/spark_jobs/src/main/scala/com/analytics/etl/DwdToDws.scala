package com.analytics.etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * DWD → DWS 天级汇总
 *
 * 把昨天的明细数据算成一行汇总指标，写入 dws_daily_metrics 表。
 *
 * ┌──────────── 汇总字段 ────────────┐
 * │ pv, uv, new_user_count,           │
 * │ avg_duration, top_city,           │
 * │ top_device, download_count,        │
 * │ search_count                      │
 * └──────────────────────────────────┘
 */
object DwdToDws {

  def execute(spark: SparkSession, dwdData: DataFrame, statDate: String): DataFrame = {
    import spark.implicits._

    println("=" * 50)
    println(s"[DWD→DWS] 开始天级汇总，日期: $statDate")

    // 过滤指定日期的数据
    val dayData = dwdData.filter(
      to_date(col("event_time")) === lit(statDate)
    ).cache()

    val totalCount = dayData.count()
    println(s"[DWD→DWS] 当天数据量: $totalCount")
    if (totalCount == 0) {
      println("[DWD→DWS] ⚠️ 当天无数据，跳过")
      return spark.emptyDataFrame
    }

    // ── 计算各项指标 ──

    // PV: 所有事件总数
    val pv = totalCount

    // UV: 独立用户数
    val uv = dayData.select("user_id").distinct().count()

    // 新增用户（当天第一次出现的 user_id）
    // 简化版：无法和全量历史对比，这里用当天去重用户数近似
    // 生产环境：需要和昨天的用户表做 LEFT ANTI JOIN
    val newUsers = uv  // 简化处理

    // 平均停留时长（只对 page_view 事件）
    val avgDur = dayData
      .filter(col("event_type") === "page_view")
      .agg(avg("duration").cast("double"))
      .collect()(0).getDouble(0)
    val avgDuration = if (avgDur.isNaN) 0.0 else Math.round(avgDur * 100.0) / 100.0

    // 最活跃城市
    val topCity = dayData
      .groupBy("city")
      .count()
      .orderBy(desc("count"))
      .select("city")
      .first()
      .getString(0)

    // 最常见设备
    val topDevice = dayData
      .groupBy("device_model")
      .count()
      .orderBy(desc("count"))
      .select("device_model")
      .first()
      .getString(0)

    // 下载次数
    val downloadCount = dayData
      .filter(col("event_type") === "app_download")
      .count()

    // 搜索次数
    val searchCount = dayData
      .filter(col("event_type") === "search_query")
      .count()

    // ── 组装结果 ──
    val result = spark.createDataFrame(Seq(
      DailyMetric(
        stat_date = java.sql.Date.valueOf(statDate),
        event_type = "all",
        pv = pv,
        uv = uv,
        new_user_count = newUsers,
        avg_duration = avgDuration,
        top_city = topCity,
        top_device = topDevice,
        download_count = downloadCount,
        search_count = searchCount
      )
    ))

    println(s"[DWD→DWS] 汇总完成: PV=$pv, UV=$uv, 下载=$downloadCount")
    println("=" * 50)

    result
  }
}

/** 天级汇总指标 */
case class DailyMetric(
  stat_date: java.sql.Date,
  event_type: String,
  pv: Long,
  uv: Long,
  new_user_count: Long,
  avg_duration: Double,
  top_city: String,
  top_device: String,
  download_count: Long,
  search_count: Long
)
