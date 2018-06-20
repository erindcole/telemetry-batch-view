/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.mozilla.telemetry.utils.UDFs.MozUDFs
import com.mozilla.telemetry.utils.getOrCreateSparkSession
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.DataFrame
import org.rogach.scallop._

object GenericDauView {
  private val jobName: String = "generic_dau"

  class Conf(args: Array[String]) extends ScallopConf(args) {
    val date = opt[String](
      "date",
      descr = "submission date to process",
      required = true)
    val input = opt[String](
      "input",
      descr = "location to read client_count dataset from",
      required = true)
    val output = opt[String](
      "output",
      descr = "location to write dau dataset to",
      required = true)

    // optional
    val columns = opt[String](
      "columns",
      descr = "calculate dau for these columns",
      required = false)
    val outlierThreshold = opt[Int](
      "outlier-threshold",
      descr = "drop rows with a count lower than this value",
      required = false)
    val topColumn = opt[String](
      "top-column",
      descr = "restrict input to top TOP_LIMIT values by count for TOP_COLUMN",
      required = false)
    val topLimit = opt[Int](
      "top-limit",
      default = Some(100),
      descr = "restrict input to top TOP_LIMIT values by count for TOP_COLUMN",
      required = false)
    val where = opt[String](
      "where",
      descr = "where clause to restrict input",
      required = false)

    verify()

    private val fmt = DateTimeFormatter.ofPattern("yyyyMMdd")
    private val fmtDashed = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    private val dateParsed = LocalDate.parse(date(), fmt)
    val dateDashed: String = dateParsed.format(fmtDashed)
    val monthDates: List[String] = (0 to 27).map(dateParsed.minusDays(_).format(fmt)).toList
    val weekDates: List[String] = (0 to 6).map(dateParsed.minusDays(_).format(fmt)).toList
  }

  def main(args: Array[String]) {
    val conf = new Conf(args)

    val inputBasePath = conf.input()
    val inputPaths = conf.monthDates.map(date=>s"$inputBasePath/submission_date=$date")
    val outputPath = s"${conf.output()}/submission_date_s3=${conf.date()}"

    val spark = getOrCreateSparkSession(jobName)
    spark.registerUDFs // for hlls

    val input = spark
      .read
      .option("mergeSchema", "true")
      .option("basePath", inputBasePath)
      .parquet(inputPaths:_*)

    List(
      applyTop _,
      applyTop _,
      applyTop _)
      .foldLeft(input){(df, f)=>f(df, conf)}
      .write
      .mode("overwrite")
      .parquet(outputPath)
  }

  def applyWhere(df: DataFrame, conf: Conf): DataFrame = conf.where.get match {
    case Some(where) => df.where(where)
    case _ => df
  }

  def applyTop(df: DataFrame, conf: Conf): DataFrame = {
    (conf.topColumn.get, conf.topLimit.get) match {
      case (Some(column), Some(limit)) =>
        val top = df
          .groupBy(column)
          .agg(expr("hll_cardinality(hll_merge(hll)) as count"))
          .orderBy("count")
          .limit(limit)
          .select(column)
        df.join(top, column)
      case _ => df
    }
  }

  def extractDay(df: DataFrame, conf: Conf): DataFrame = {
    val date = conf.date()
    val cols = Array("submission_date") ++ (conf.columns.get match {
      case Some(csv) => csv.split(",")
      case _ => Array[String]()
    })
    // reduce columns and merge hlls
    val sample = df
      .groupBy(cols.head, cols.tail:_*)
      .agg(expr("hll_merge(hll) as hll"))

    val dau = sample
      // only compute dau for today
      .where(s"submission_date='$date'")
      // compute dau
      .groupBy(cols.head, cols.tail:_*)
      .agg(expr("hll_cardinality(hll_merge(hll)) as dau"))
      .drop("hll")

    val mau = sample // already filtered to monthDates
      // set the submission_date for this mau
      .withColumn("submission_date", expr(s"'$date'"))
      // compute mau
      .groupBy(cols.head, cols.tail:_*)
      .agg(expr("hll_cardinality(hll_merge(hll)) as mau"))
      .drop("hll")

    val smoothed_dau = sample
      // filter to weekDates
      .filter(col("submission_date").isin(conf.weekDates))
      // set the submission_date for this smoothed dau
      .withColumn("submission_date", expr(s"'$date'"))
      // compute smoothed dau
      .groupBy(cols.head, cols.tail:_*)
      .agg(expr("avg(hll_cardinality(hll)) as smoothed_dau"))
      .drop("hll")

    dau
      // join mau and smoothed_dau
      .join(mau, cols)
      .join(smoothed_dau, cols)
      // drop this column, it will be in s3
      .drop("submission_date")
      // create date column formatted for redash
      .withColumn("submission_date_dashed", expr(s"'${conf.dateDashed}'"))
      // create ER column
      .withColumn("er", expr("smoothed_dau/mau"))
  }
}
