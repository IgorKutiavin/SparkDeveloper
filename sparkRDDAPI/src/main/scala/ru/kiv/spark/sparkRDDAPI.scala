package ru.kiv.spark

import com.fasterxml.jackson.module.scala.ScalaObjectMapper
import org.apache.spark.{SparkConf, SparkContext}
import io.circe.parser._
import org.apache.hadoop.shaded.org.codehaus.jackson.map.ObjectMapper
import org.apache.spark.rdd.RDD

import java.io.{File, PrintWriter}
import scala.util.Random

object sparkRDDAPI {
  def main(args: Array[String]): Unit = {

    // Создаём конекст исполнения
    val conf = new SparkConf().setAppName("SparkRDDAPI").setMaster("local[*]")
    val sc   = new SparkContext(conf)
    var i = 0
    val tripRDD = sc.textFile(args(0)).filter(x => x.split(",")(0) != "VendorID")
      .map(x =>
        (tripData.getKey(x),tripData.getData(x))
    )
    val taxiRDD = sc.textFile(args(1)).filter(_.split(",")(0).replace("\"","") != "LocationID")
      .map(x =>
        (taxiData.getKey(x), taxiData.getData(x))
    )

    val joinRDD = tripRDD.join(taxiRDD)

    val groupRDD = joinRDD.groupBy(x => (x._2._2.Zone,(x._2._1.tpep_dropoff_datetime.hourOfDay().get())))

//    groupRDD.take(2).foreach(println)
//    println(s"tripRDD.count: ${tripRDD.count().toString}\ntaxiRDD.count: ${taxiRDD.count().toString}\njoinRDD.count: ${joinRDD.count().toString}")

    val headRDD = sc.parallelize(List("Zone,hour,count"))
    val outRDD = headRDD.union(groupRDD.map(x => (x._1._1 + "," + x._1._2.toString + "," + x._2.count(p => p._1 == p._1).toString)))

    outRDD.coalesce(2).saveAsTextFile(args(2))

  }
}