package com.analytics

import com.analytics.etl.{DwdToDws, OdsToDwd}
import com.analytics.metrics.{FunnelCalc, RetentionCalc, UserProfile}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * Spark 离线批处理 — 主入口
 *
 * ┌────────── 执行流程 ──────────┐
 * │                              │
 * │ 1. ODS → DWD 数据清洗         │
 * │ 2. DWD → DWS 天级汇总         │
 * │ 3. 留存分析                   │
 * │ 4. 漏斗分析                   │
 * │ 5. 用户画像                   │
 * │                              │
 * │ 所有结果写入 ClickHouse       │
 * └──────────────────────────────┘
 *
 * 运行方式：
 *   spark-submit \
 *     --class com.analytics.Main \
 *     --master local[4] \
 *     target/spark-jobs-1.0.0.jar \
 *     --date 2026-07-10
 */
object Main {

  // ======== ClickHouse 连接配置 ========
  private val CK_HOST = "localhost"
  private val CK_PORT = "8123"
  private val CK_DB = "analytics"
  private val CK_USER = "admin"
  private val CK_PASSWORD = "admin123"
  private lazy val CK_URL = s"jdbc:clickhouse://$CK_HOST:$CK_PORT/$CK_DB"

  def main(args: Array[String]): Unit = {

    // ── 解析命令行参数 ──
    val config = parseArgs(args)
    val date = config.getOrElse("date", LocalDate.now().minusDays(1).toString)
    // 默认处理昨天的数据（今天凌晨跑昨天的）

    println("=" * 60)
    println(s"  Spark 离线批处理作业")
    println(s"  处理日期: $date")
    println(s"  ClickHouse: $CK_HOST:$CK_PORT")
    println("=" * 60)

    // ── 创建 SparkSession ──
    val spark = SparkSession.builder()
      .appName(s"UserBehavior-Analytics-$date")
      .master("local[4]")  // 本地4线程
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()

    try {
      // ============================================================
      // Step 1: 从 ClickHouse 读取 ODS 数据
      // ============================================================
      println("\n📥 读取 ODS 数据...")
      val odsDF = readClickHouse(spark, "ods_user_behavior_log", date)

      // ============================================================
      // Step 2: ODS → DWD 清洗
      // ============================================================
      val dwdDF = OdsToDwd.execute(spark, odsDF)
      writeClickHouse(dwdDF, "dwd_user_behavior")

      // ============================================================
      // Step 3: DWD → DWS 天级汇总
      // ============================================================
      val dwsDF = DwdToDws.execute(spark, dwdDF, date)
      if (!dwsDF.isEmpty) {
        writeClickHouse(dwsDF, "dws_daily_metrics")
      }

      // ============================================================
      // Step 4: 留存分析
      // ============================================================
      val retentionDF = RetentionCalc.execute(spark, dwdDF, date)
      writeClickHouse(retentionDF, "ads_retention")

      // ============================================================
      // Step 5: 漏斗分析
      // ============================================================
      val funnelDF = FunnelCalc.execute(spark, dwdDF, date)
      writeClickHouse(funnelDF, "ads_funnel_analysis")

      // ============================================================
      // Step 6: 用户画像（全量计算）
      // ============================================================
      println("\n👤 计算用户画像（全量）...")
      val allDwd = readClickHouse(spark, "dwd_user_behavior")
      val profileDF = UserProfile.execute(spark, allDwd)
      writeClickHouse(profileDF, "ads_user_profile", mode = SaveMode.Overwrite)
      // 用户画像是全量覆盖的，每次跑都全量更新

      println("\n" + "=" * 60)
      println("  ✅ 所有 Spark 作业完成！")
      println("=" * 60)

    } finally {
      spark.stop()
    }
  }

  // ============================================================
  // 工具方法
  // ============================================================

  /** 从 ClickHouse 读数据 */
  private def readClickHouse(spark: SparkSession, table: String, date: String = null): DataFrame = {
    val options = new java.util.Properties()
    options.setProperty("user", CK_USER)
    options.setProperty("password", CK_PASSWORD)
    options.setProperty("driver", "com.clickhouse.jdbc.ClickHouseDriver")

    var sql = s"(SELECT * FROM $CK_DB.$table"
    if (date != null && !date.isEmpty) {
      sql += s" WHERE toDate(event_time) = '$date'"
    }
    sql += ") AS t"

    spark.read.jdbc(CK_URL, sql, options)
  }

  /** 写数据到 ClickHouse */
  private def writeClickHouse(
    df: DataFrame,
    table: String,
    mode: SaveMode = SaveMode.Append
  ): Unit = {
    val options = new java.util.Properties()
    options.setProperty("user", CK_USER)
    options.setProperty("password", CK_PASSWORD)
    options.setProperty("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    // ClickHouse 批量写入优化
    options.setProperty("batchsize", "10000")
    options.setProperty("socket_timeout", "300000")

    df.write
      .mode(mode)
      .jdbc(CK_URL, s"$CK_DB.$table", options)

    println(s"📤 已写入 ClickHouse 表: $table")
  }

  /** 命令行参数解析（简易版） */
  private def parseArgs(args: Array[String]): Map[String, String] = {
    var config = Map[String, String]()
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--date" if i + 1 < args.length =>
          config += ("date" -> args(i + 1)); i += 2
        case _ => i += 1
      }
    }
    config
  }
}
