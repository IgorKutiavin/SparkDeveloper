package ru.kiv.spark

import com.fasterxml.jackson.module.scala.ScalaObjectMapper
import org.apache.hadoop.shaded.org.codehaus.jackson.map.ObjectMapper
import org.joda.time
import org.joda.time.DateTime

import java.time.format.DateTimeFormatter

//case class tripData (
//                      VendorID: Int,
//                      tpep_pickup_datetime: String,
//                      tpep_dropoff_datetime: String,
//                      passenger_count: Int,
//                      trip_distance: Double,
//                      pickup_longitude: Double,
//                      pickup_latitude: Double,
//                      RatecodeID: Int,
//                      store_and_fwd_flag: String,
//                      dropoff_longitude: Double,
//                      dropoff_latitude:Double,
//                      payment_type: Int,
//                      fare_amount:Double,
//                      extra:Double,
//                      mta_tax:Double,
//                      tip_amount:Double,
//                      tolls_amount:Double,
//                      improvement_surcharge:Double,
//                      total_amount:Double
//                    )

case class tripData (
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
object tripData {

  def getData(s: String):tripData = {
    val datetime_format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
    val a = s.split(",")
    if (a.length == 19) {
      tripData(
        a(0).toInt,
//        DateTime.parse(datetime_format.parse(a(1).replace("T"," ")).toString),
//        DateTime.parse(datetime_format.parse(a(2).replace("T"," ")).toString),
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