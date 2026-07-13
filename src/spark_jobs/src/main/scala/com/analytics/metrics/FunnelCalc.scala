package com.analytics.metrics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 离线漏斗分析
 *
 * 和 Flink 实时漏斗的区别：
 *   实时漏斗 → 宏观计数（每个窗口独立统计各事件的数量）
 *   离线漏斗 → 可以做到用户级别的精确漏斗
 *              （同一个用户是否完成了漏斗的全部步骤）
 *
 * 漏斗步骤：曝光 → 点击 → 下载 → 安装
 */
object FunnelCalc {

  /** 漏斗定义：依次经过的步骤 */
  private val FUNNEL_STEPS = Seq(
    ("impression", "ad_impression"),
    ("click", "ad_click"),
    ("download", "app_download"),
    ("install", "app_install")
  )

  /**
   * @param dwdData  DWD 明细数据
   * @param date     统计日期
   */
  def execute(spark: SparkSession, dwdData: DataFrame, date: String): DataFrame = {
    import spark.implicits._

    println("=" * 50)
    println(s"[漏斗分析] 日期: $date")

    // ── Step 1: 过滤当天漏斗相关事件 ──
    val funnelTypes = FUNNEL_STEPS.map(_._2)
    val dayData = dwdData
      .filter(to_date(col("event_time")) === lit(date))
      .filter(col("event_type").isin(funnelTypes: _*))
      .select("user_id", "event_type")
      .cache()

    println(s"[漏斗分析] 漏斗相关事件数: ${dayData.count()}")

    // ── Step 2: 统计每一步的独立用户数 ──
    val stepCounts = FUNNEL_STEPS.map { case (stepName, eventType) =>
      val count = dayData
        .filter(col("event_type") === eventType)
        .select("user_id")
        .distinct()
        .count()
      (stepName, count)
    }

    // ── Step 3: 计算转化率 ──
    val baseCount = stepCounts.head._2.toDouble  // 第一步 = 100%

    val results = stepCounts.zipWithIndex.map { case ((stepName, count), idx) =>
      val prevCount = if (idx == 0) count else stepCounts(idx - 1)._2
      val stepRate = if (prevCount > 0)
        Math.round(count * 10000.0 / prevCount) / 100.0
      else 0.0
      val overallRate = if (baseCount > 0)
        Math.round(count * 10000.0 / baseCount) / 100.0
      else 0.0

      println(s"[漏斗分析] $stepName: $count (步骤转化率=$stepRate%, 总转化率=$overallRate%)")

      (java.sql.Date.valueOf(date), "app_download_funnel",
        idx + 1, stepName, count, stepRate, overallRate)
    }

    // ── Step 4: 组装输出 ──
    val resultDF = spark.createDataFrame(results).toDF(
      "stat_date", "funnel_name", "step_order",
      "step_name", "user_count", "step_rate", "overall_rate"
    )

    println("=" * 50)

    resultDF
  }
}
