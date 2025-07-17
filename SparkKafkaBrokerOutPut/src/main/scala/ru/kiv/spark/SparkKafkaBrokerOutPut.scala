package ru.kiv.spark

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

object SparkKafkaBrokerOutPut {
  def main (args: Array[String]) = {

    val config                 = ConfigFactory.load()
    val pathCSVFile            = config.getString("path_csv_file")
    val outputBootstrapServers  = config.getString("output.bootstrap.servers")
    val outputTopic             = config.getString("output.topic")
    val checkPointParh         = config.getString("checkpoint_path")
    val csvPath                = config.getString("csv_files_path")

    val spark = SparkSession.builder
      .appName("SparkKafkaBrokerOutPut")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", outputBootstrapServers)
      .option("subscribe", outputTopic)
      .load()
      .selectExpr("cast(value as String)")
      .as[String]

//    df.printSchema()
//    df.writeStream
//      .option("checkpointLocation", checkPointParh)
//      .format("parquet")
//      .toTable("userRaiting")
//      .awaitTermination()

    spark.read.table("userraiting").show(10)
  }
}