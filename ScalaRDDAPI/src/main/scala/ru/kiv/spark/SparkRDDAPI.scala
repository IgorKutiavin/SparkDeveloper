package ru.kiv.spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDAPI {

   def app(): Unit = {

     // Создаём конекст исполнения
     val conf = new SparkConf().setAppName("SparkRDDAPI").setMaster("local[*]")
     val sc   = new SparkContext(conf)

     val tripRDD = sc.textFile("")

   }

}
