package ru.kiv.spark

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object MonitoringWorkflow {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("MonitoringWorkflow")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val link_param = spark.read
      .option("header", "true") // Если первая строка - заголовок
      .option("inferSchema", "true") // Автоматическое определение типов данных
      .option("delimiter", ";") // Разделитель, по умолчанию - запята
      .csv("src/main/resources/logs/parent_wf_operation_list.csv")
      .as[Dependency].persist()

    val obj_dependency = Dependency(link_param)(spark)
    obj_dependency.printSchema()
    //obj_dependency.where("tbl_trg in ('i$cam.v$soi_vodnal','i$cam.v$soi_akciz_1151095','i$cam.v$soi_lic_zhm')").show(false)

    val l = """i$cam.v$soi_vodnal,i$cam.v$soi_akciz_1151095,i$cam.v$soi_lic_zhm""".split(",").toList

    val listTbl = TableCalc(obj_dependency, l)(spark)

    listTbl.printSchema()
    listTbl.orderBy("tbl_calc_nme","dependency_lvl").show(false)
    println(listTbl.count())


  }
}