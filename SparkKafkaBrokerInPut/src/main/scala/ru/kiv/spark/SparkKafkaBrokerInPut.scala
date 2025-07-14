package ru.kiv.spark

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

object SparkKafkaBrokerInPut {

  def main(args: Array[String]): Unit = {

    val config                 = ConfigFactory.load()
    val pathCSVFile            = config.getString("path_csv_file")
    val inputBootstrapServers  = config.getString("input.bootstrap.servers")
    val inputTopic             = config.getString("input.topic")
    val checkPointParh         = config.getString("checkpoint_path")
    val csvPath                = config.getString("csv_files_path")

    val spark = SparkSession.builder
      .appName("SparkKafkaBrokerInPut")
      .master("local[*]")
      .getOrCreate()

    val Bestsellers = new StructType()
      .add("name", "string")
      .add("Author", "string")
      .add("User Rating", "double")
      .add("Reviews", "integer")
      .add("Price", "integer")
      .add("Year", "integer")
      .add("Genre", "string")

    val df = spark.readStream
      .option("header", "true") // Если первая строка - заголовок
      .option("inferSchema", "true") // Автоматическое определение типов данных
      .option("delimiter", ",") // Разделитель, по умолчанию - запята
      .schema(Bestsellers)
      .csv(csvPath)


    val js = df.toJSON

    val query = js.writeStream
      .outputMode("append")
      .format("kafka")
      .option("kafka.bootstrap.servers", inputBootstrapServers)
      .option("topic", inputTopic)
      .option("checkpointLocation", checkPointParh)
      .start()

    query.awaitTermination()
  }
}