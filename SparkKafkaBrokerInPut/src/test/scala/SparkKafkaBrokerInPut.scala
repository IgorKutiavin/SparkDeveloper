package ru.kiv.spark

import com.typesafe.config.ConfigFactory
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkKafkaBrokerInPut {

  def main(args: Array[String]) = {

    val config                 = ConfigFactory.load()
    val pathCSVFile            = config.getString("")
    val inputBootstrapServers  = config.getString("input.bootstrap.servers")
    val inputTopic             = config.getString("input.topic")
    val outputBootstrapServers = config.getString("output.bootstrap.servers")
    val outputTopic            = config.getString("output.topic")
    val path2model             = config.getString("path2model")
    val checkpointLocation     = config.getString("checkpointLocation")

    val spark = SparkSession.builder
      .appName("SparkKafkaBrokerInPut")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read
      .option("header", "true") // Если первая строка - заголовок
      .option("inferSchema", "true") // Автоматическое определение типов данных
      .option("delimiter", ",") // Разделитель, по умолчанию - запята
      .csv(args(1))

  }
}