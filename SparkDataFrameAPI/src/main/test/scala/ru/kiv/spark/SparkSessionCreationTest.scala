package ru.kiv.spark

import org.apache.spark.sql.SparkSession

trait SparkSessionCreationTest {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("SparkDataFrameTest")
    .master("local[*]")
    .getOrCreate()
}
