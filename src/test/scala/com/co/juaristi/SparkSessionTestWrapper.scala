package com.co.juaristi

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
  }
  val sc = spark.sparkContext
  sc.hadoopConfiguration.set("fs.s3a.access.key", "AKIAIBXSZZ5DF56OT5WQ")
  sc.hadoopConfiguration.set("fs.s3a.secret.key", "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")

}
