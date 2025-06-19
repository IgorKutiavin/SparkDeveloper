package ru.kiv.spark

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkDataSetAPI {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("SparkDataSetAPI")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val dsTaxi = spark.read
      .parquet(args(0)).toDF()
      .as[TaxiRoad]

    val dsZone = spark.read
      .option("header", "true") // Если первая строка - заголовок
      .option("inferSchema", "true") // Автоматическое определение типов данных
      .option("delimiter", ",") // Разделитель, по умолчанию - запята
      .csv(args(1))
      .as[TaxiZone]

    val dsRez = dsTaxi.join(dsZone,expr("PULocationID = LocationID"),joinType = "left")
      .select("PULocationID","trip_distance","Zone")
      .groupBy("Zone","PULocationID")
      .agg(count("Zone").as("CountTrip")
        ,min("trip_distance").as("min_trip_distance")
        ,avg("trip_distance").as("avg_trip_distance")
        ,max("trip_distance").as("max_trip_distance")
        ,sqrt((col("min_trip_distance")+col("avg_trip_distance")+col("max_trip_distance"))/3).as("rmse_trip_distance"))
      .select(col("Zone"),col("CountTrip"),col("min_trip_distance").cast("decimal(10,2)"),col("avg_trip_distance").cast("decimal(10,2)"),col("max_trip_distance").cast("decimal(10,2)"),col("rmse_trip_distance").cast("decimal(10,2)"))

    dsRez.write
      .parquet(args(2))
  }
}