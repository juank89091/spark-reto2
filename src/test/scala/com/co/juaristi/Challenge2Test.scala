package com.co.juaristi

import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalatest.FunSuite

class Challenge2Test  extends FunSuite with SparkSessionTestWrapper with Challenge2 {
  import spark.implicits._
  spark.sparkContext.setLogLevel("DEBUG")

  test("last6monthTrades") {

    val csv: DataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .load("s3a://deutsche-boerse-xetra-pds/2020-02-13/")

    last6monthTrades(csv)
  }

  test("lastThreeMonthTradesAveragePrice") {

    val csv: DataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .load("s3a://deutsche-boerse-xetra-pds/2020-02-13/")

    lastThreeMonthTradesAveragePrice(csv)
  }

  test("openingClosingPriceTrades") {

    val csv: DataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .load("s3a://deutsche-boerse-xetra-pds/2020-02-13/")

    openingClosingPriceTrades(csv, List("BNR"))
  }
}
