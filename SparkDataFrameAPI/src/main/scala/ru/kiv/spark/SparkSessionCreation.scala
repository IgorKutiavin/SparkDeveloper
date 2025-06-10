package ru.kiv.spark

import org.apache.spark.sql.SparkSession

trait SparkSessionCreation {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("SparkDataFrame")
    .master("local[*]")
    .getOrCreate()
}
