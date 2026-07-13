package com.analytics.metrics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 用户留存计算
 *
 * ┌──────── 什么是留存 ────────┐
 * │ 7月10日来了1000个新用户，     │
 * │ 其中400人在7月11日又来了     │ → 次日留存率 40%
 * │ 其中250人在7月13日又来了     │ → 3日留存率 25%
 * │ 其中150人在7月17日又来了     │ → 7日留存率 15%
 * │ 其中80人在8月10日又来了      │ → 30日留存率 8%
 * └────────────────────────────┘
 *
 * 算法：
 *  1. 找出基准日所有活跃用户
 *  2. 对每个用户，检查他在第 N 天后是否还有活跃记录
 *  3. 留存率 = 第 N 天回来的用户数 / 基准日用户数
 *
 * 面试考点：
 *  - 留存计算的时间复杂度（自连接）
 *  - 如何优化大数据量下的留存计算
 */
object RetentionCalc {

  /**
   * @param dwdData  DWD 明细数据
   * @param baseDate 基准日期，如 "2026-07-10"
   * @param retDays  要计算的留存天数，如 Seq(1, 3, 7, 30)
   */
  def execute(
    spark: SparkSession,
    dwdData: DataFrame,
    baseDate: String,
    retDays: Seq[Int] = Seq(1, 3, 7, 30)
  ): DataFrame = {
    import spark.implicits._

    println("=" * 50)
    println(s"[留存分析] 基准日期: $baseDate, 留存天数: $retDays")

    // ── Step 1: 基准日活跃用户 ──
    val baseUsers = dwdData
      .filter(to_date(col("event_time")) === lit(baseDate))
      .select("user_id")
      .distinct()

    val baseUserCount = baseUsers.count()
    println(s"[留存分析] 基准日用户数: $baseUserCount")

    // ── Step 2: 后续日期的活跃用户 ──
    // 后续所有日期的数据
    val laterData = dwdData
      .filter(to_date(col("event_time")) > lit(baseDate))
      .select(col("user_id"), to_date(col("event_time")).as("active_date"))
      .distinct()

    // ── Step 3: 对每个留存天数计算 ──
    val results = retDays.map { day =>
      // 第 N 天的日期
      val targetDate = java.sql.Date.valueOf(
        java.time.LocalDate.parse(baseDate).plusDays(day)
      ).toString

      // 找出在 targetDate 活跃的基准日用户
      val retainedCount = baseUsers
        .join(
          laterData.filter(col("active_date") === lit(targetDate)),
          "user_id"
        )
        .count()

      val rate = if (baseUserCount > 0)
        Math.round(retainedCount * 10000.0 / baseUserCount) / 100.0
      else 0.0

      println(s"[留存分析] 第${day}天 ($targetDate): $retainedCount/$baseUserCount = $rate%")

      (java.sql.Date.valueOf(baseDate), day, baseUserCount, retainedCount, rate)
    }

    // ── Step 4: 组装输出 ──
    val resultDF = spark.createDataFrame(results).toDF(
      "first_date", "ret_day", "new_user_count", "retained_count", "retention_rate"
    )

    println("=" * 50)

    resultDF
  }
}
