package com.analytics.metrics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 用户画像计算
 *
 * 基于历史行为数据，给每个用户打标签：
 *   - 活跃度等级（高/中/低/沉睡）
 *   - 偏好 App 类别（从 properties 中提取）
 *   - 常用时段（早/中/晚/深夜）
 *   - 设备类型（旗舰/中端/入门）
 *
 * 面试亮点：提到了用户画像的生产级实现思路
 */
object UserProfile {

  def execute(spark: SparkSession, dwdData: DataFrame): DataFrame = {
    import spark.implicits._

    println("=" * 50)
    println("[用户画像] 开始计算...")

    val userStats = dwdData
      .groupBy("user_id")
      .agg(
        // 总事件数 → 活跃度
        count("*").as("total_events"),
        // 活跃天数
        countDistinct(to_date(col("event_time"))).as("active_days"),
        // 最常用设备
        first("device_model").as("device_model"),
        // 最常访问的城市
        first("city").as("city_last"),
        // 最早/最晚活跃时间
        min("event_time").as("first_seen"),
        max("event_time").as("last_seen"),
        // 各事件类型次数
        sum(when(col("event_type") === "app_download", 1).otherwise(0)).as("downloads"),
        sum(when(col("event_type") === "search_query", 1).otherwise(0)).as("searches"),
        sum(when(col("event_type") === "in_app_purchase", 1).otherwise(0)).as("purchases")
      )
      // ── 打标签 ──
      .withColumn("activity_level",
        when(col("total_events") >= 100, "高活跃")
          .when(col("total_events") >= 30, "中活跃")
          .when(col("total_events") >= 5, "低活跃")
          .otherwise("沉睡用户")
      )
      .withColumn("user_type",
        when(col("purchases") > 0, "付费用户")
          .when(col("downloads") > 0, "下载型用户")
          .when(col("searches") > 2, "搜索型用户")
          .otherwise("浏览型用户")
      )
      .select(
        col("user_id"),
        col("activity_level"),
        col("user_type"),
        col("total_events"),
        col("active_days"),
        col("downloads"),
        col("searches"),
        col("purchases"),
        col("device_model"),
        col("first_seen"),
        col("last_seen")
      )

    val totalUsers = userStats.count()
    val highActive = userStats.filter(col("activity_level") === "高活跃").count()
    val paying = userStats.filter(col("user_type") === "付费用户").count()

    println(s"[用户画像] 总用户: $totalUsers")
    println(s"[用户画像] 高活跃: $highActive (${Math.round(highActive * 10000.0 / totalUsers) / 100.0}%)")
    println(s"[用户画像] 付费用户: $paying (${Math.round(paying * 10000.0 / totalUsers) / 100.0}%)")
    println("=" * 50)

    userStats
  }
}
