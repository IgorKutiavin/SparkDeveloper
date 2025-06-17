package ru.kiv.spark

import org.joda.time.DateTime

case class TripData (
                      VendorID: Int,
                      tpep_pickup_datetime: DateTime,
                      tpep_dropoff_datetime:DateTime,
                      passenger_count: Int,
                      trip_distance:Double,
                      RatecodeID: Int,
                      store_and_fwd_flag:String,
                      PULocationID: Int,
                      DOLocationID: Int,
                      payment_type:Int,
                      fare_amount:Double,
                      extra:Double,
                      mta_tax:Double,
                      tip_amount:Double,
                      tolls_amount:Double,
                      improvement_surcharge:Double,
                      total_amount:Double,
                      congestion_surcharge:Double,
                      Airport_fee:Double
                   )
object TripData {

  def apply(s: String):TripData = {
    val a = s.split(",")
    if (a.length == 19) {
      TripData(
        a(0).toInt,
        DateTime.parse(a(1)),
        DateTime.parse(a(2)),
        a(3).toInt,
        a(4).toDouble,
        a(5).toInt,
        a(6),
        a(7).toInt,
        a(8).toInt,
        a(9).toInt,
        a(10).toDouble,
        a(11).toDouble,
        a(12).toDouble,
        a(13).toDouble,
        a(14).toDouble,
        a(15).toDouble,
        a(16).toDouble,
        a(17).toDouble,
        a(18).toDouble)
    }
    else
      {
        tripData(
          -1,
          DateTime.parse("1900-01-01T00:00:00.000"),
          DateTime.parse("1900-01-01T00:00:00.000"),
          1,
          1.0,
          1,
          "",
          1,
          1,
          1,
          1.0,
          1.0,
          1.0,
          1.0,
          1.0,
          1.0,
          1.0,
          1.0,
          1.0)
      }
  }

  def getKey(s: String): Int = {
    val a = s.split(",")
    if (a.length == 19)
      a(7).toInt
    else
      0
  }

}