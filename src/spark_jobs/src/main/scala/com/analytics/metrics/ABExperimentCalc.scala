package com.analytics.metrics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * AB 实验指标计算
 *
 * 按 experiment_id + variant 分组，计算各组核心指标，
 * 然后计算实验组相对对照组的提升幅度。
 * 输出: ads_ab_experiment 表
 */
object ABExperimentCalc {

  def execute(spark: SparkSession, dwdDF: DataFrame, date: String): DataFrame = {
    import spark.implicits._

    println("\n🧪 AB 实验指标计算...")

    val expDF = dwdDF
      .where(col("experiment_id") =!= "" && col("variant") =!= "")
      .where(to_date(col("event_time")) === date)

    if (expDF.isEmpty) {
      println("⚠️ 没有 AB 实验数据，跳过")
      return spark.emptyDataFrame
    }

    // ===== 1. PV =====
    val pvMetrics: DataFrame = expDF
      .groupBy("experiment_id", "variant")
      .agg(
        count("*").as("metric_value"),
        countDistinct("user_id").as("user_count")
      )
      .withColumn("metric_name", lit("pv"))
      .withColumn("metric_unit", lit("次"))

    // ===== 2. UV =====
    val uvMetrics: DataFrame = expDF
      .groupBy("experiment_id", "variant")
      .agg(
        countDistinct("user_id").as("metric_value"),
        countDistinct("user_id").as("user_count")
      )
      .withColumn("metric_name", lit("uv"))
      .withColumn("metric_unit", lit("人"))

    // ===== 3. 下载量 =====
    val downloadMetrics: DataFrame = expDF
      .where(col("event_type") === "app_download")
      .groupBy("experiment_id", "variant")
      .agg(
        count("*").as("metric_value"),
        countDistinct("user_id").as("user_count")
      )
      .withColumn("metric_name", lit("download_count"))
      .withColumn("metric_unit", lit("次"))

    // ===== 4. 下载转化率 =====
    val totalUsersDF: DataFrame = expDF
      .groupBy("experiment_id", "variant")
      .agg(countDistinct("user_id").as("total_users"))

    val downloadUsersDF: DataFrame = expDF
      .where(col("event_type") === "app_download")
      .groupBy("experiment_id", "variant")
      .agg(countDistinct("user_id").as("download_users"))

    val conversionMetrics: DataFrame = totalUsersDF
      .join(downloadUsersDF, Seq("experiment_id", "variant"), "left")
      .withColumn("metric_value",
        round(col("download_users").cast("double") / col("total_users") * 100, 2))
      .withColumn("user_count", col("total_users"))
      .select("experiment_id", "variant", "metric_value", "user_count")
      .withColumn("metric_name", lit("download_conversion_rate"))
      .withColumn("metric_unit", lit("%"))

    // ===== 5. 搜索量 =====
    val searchMetrics: DataFrame = expDF
      .where(col("event_type") === "search_query")
      .groupBy("experiment_id", "variant")
      .agg(
        count("*").as("metric_value"),
        countDistinct("user_id").as("user_count")
      )
      .withColumn("metric_name", lit("search_count"))
      .withColumn("metric_unit", lit("次"))

    // ===== 6. 平均停留时长 =====
    val durationMetrics: DataFrame = expDF
      .where(col("event_type") === "page_view")
      .groupBy("experiment_id", "variant")
      .agg(
        round(avg("duration"), 0).as("metric_value"),
        countDistinct("user_id").as("user_count")
      )
      .withColumn("metric_name", lit("avg_duration"))
      .withColumn("metric_unit", lit("ms"))

    // ===== 合并所有指标 =====
    val allMetrics: DataFrame = pvMetrics
      .union(uvMetrics)
      .union(downloadMetrics)
      .union(conversionMetrics)
      .union(searchMetrics)
      .union(durationMetrics)

    // ===== 计算 uplift =====
    val upliftDF: DataFrame = allMetrics
      .groupBy("experiment_id", "metric_name")
      .agg(
        max(when(col("variant") === "A", col("metric_value"))).as("a_value"),
        max(when(col("variant") === "B", col("metric_value"))).as("b_value")
      )
      .withColumn("uplift_pct",
        when(col("a_value") > 0,
          round((col("b_value") - col("a_value")) / col("a_value") * 100, 2)
        ).otherwise(0)
      )

    // ===== 组装最终结果 =====
    val result: DataFrame = allMetrics
      .join(upliftDF, Seq("experiment_id", "metric_name"), "left")
      .select(
        col("experiment_id"),
        lit(date).cast("date").as("stat_date"),
        col("variant"),
        col("metric_name"),
        col("metric_value"),
        col("metric_unit"),
        col("user_count"),
        lit(-1.0).as("p_value"),
        lit(0).as("is_significant"),
        coalesce(col("uplift_pct"), lit(0.0)).as("uplift"),
        lit("").as("sample_data")
      )

    val rowCount = result.count()
    println(s"✅ AB 实验指标计算完成: $rowCount 条记录")
    result.show(20, truncate = false)

    result
  }
}
