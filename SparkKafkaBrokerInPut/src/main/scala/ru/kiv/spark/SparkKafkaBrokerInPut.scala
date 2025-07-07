package ru.kiv.spark

import com.typesafe.config.ConfigFactory
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.impl.StaticLoggerBinder
import org.apache.log4j.{Level, Logger}

object SparkKafkaBrokerInPut {

  def main(args: Array[String]): Unit = {

    val config                 = ConfigFactory.load()
    val pathCSVFile            = config.getString("path_csv_file")
    //val inputBootstrapServers  = config.getString("input.bootstrap.servers")
    //val inputTopic             = config.getString("input.topic")
    val outputBootstrapServers = config.getString("output.bootstrap.servers")
    val outputTopic            = config.getString("output.topic")
    //val path2model             = config.getString("path2model")
    //val checkpointLocation     = config.getString("checkpointLocation")

    //import spark.implicits._

    val spark = SparkSession.builder
      .appName("SparkKafkaBrokerInPut")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true") // Если первая строка - заголовок
      .option("inferSchema", "true") // Автоматическое определение типов данных
      .option("delimiter", ",") // Разделитель, по умолчанию - запята
      .csv(pathCSVFile)

    val json = df.toJSON
    json.show(10)

    val query = json.select(col("value")).writeStream
      .format("kafka")
      .outputMode("append")
      .format("kafka")
      .option("kafka.bootstrap.servers", outputBootstrapServers)
      .option("topic", outputTopic)
      .start()

    query.awaitTermination()
  }
}