package ru.kiv.spark

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.math.sqrt

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

      val dsRez = dsTaxi.joinWith(dsZone,col("PULocationID") === col("LocationID"))
        .map(x => TaxiTrip(x._2.Zone,x._1.trip_distance))
        .groupByKey(_.Zone)
        .flatMapGroups { case (key, items) =>
          items match {
            case Iterator.empty =>
              Seq(TaxiTripAgg(key, 0,0,0.0,0.0,0.0))
            case _ =>
              val ttArray = items.toArray
              val zoneCount = ttArray.length
              val min_trip_distance = ttArray.map(_.trip_distance).min
              val max_trip_distance = ttArray.map(_.trip_distance).max
              val avg_trip_distance = aggTrip(min_trip_distance, max_trip_distance)
              val rmse_trip_distance = rmseTrip(min_trip_distance, max_trip_distance, avg_trip_distance)
              Seq(TaxiTripAgg(key,zoneCount,min_trip_distance,max_trip_distance,avg_trip_distance, rmse_trip_distance))
          }
        }.as[TaxiTripAgg].toDF()

    dsRez.write
      .parquet(args(2))
  }
  private def aggTrip(x: Double,y: Double):Double = {
    (x + y)/2
  }

  private def rmseTrip(x: Double, y: Double, z: Double): Double = {
    sqrt((x + y + z)/3)
  }

}

