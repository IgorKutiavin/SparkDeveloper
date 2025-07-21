package ru.kiv.spark

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods.parse

object SparkKafkaBrokerOutPut {
  def main (args: Array[String]) = {

    val config                 = ConfigFactory.load()
    val outputBootstrapServers  = config.getString("output.bootstrap.servers")
    val outputTopic             = config.getString("output.topic")
    val checkPointParh         = config.getString("checkpoint_path")
    val filePath                = config.getString("files_path")

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

    val d2 = df.filter(record => {
      implicit val formats: DefaultFormats.type = DefaultFormats
      val bs = parse(record).extract[Bestsellers]
      bs.`User Rating` < 4
    })

//    d2.writeStream
//      .outputMode("append")
//      .format("console")
//      .start()
//      .awaitTermination()

    d2.printSchema()
    d2.writeStream
      .option("checkpointLocation", checkPointParh)
      .format("parquet")
      .start(filePath)
      .awaitTermination()
  }
}