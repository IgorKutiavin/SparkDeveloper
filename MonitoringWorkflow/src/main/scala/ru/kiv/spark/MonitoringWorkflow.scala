package ru.kiv.spark

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object MonitoringWorkflow {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("MonitoringWorkflow")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val parent_list = spark.read
      .option("header", "true") // Если первая строка - заголовок
      .option("inferSchema", "true") // Автоматическое определение типов данных
      .option("delimiter", ";") // Разделитель, по умолчанию - запята
      .csv("src/main/resources/logs/parent_wf_operation_list.csv")
      .persist()

    val wf = spark.read
      .option("header", "true") // Если первая строка - заголовок
      .option("inferSchema", "true") // Автоматическое определение типов данных
      .option("delimiter", ";") // Разделитель, по умолчанию - запята
      .csv("src/main/resources/logs/workflow.csv")


    val tsk = spark.read
      .option("header", "true") // Если первая строка - заголовок
      .option("inferSchema", "true") // Автоматическое определение типов данных
      .option("delimiter", ";") // Разделитель, по умолчанию - запята
      .csv("src/main/resources/logs/task.csv")


    val w = Window.partitionBy("wf_nme_unq","tsk_nme","rls_stus").orderBy(desc("rls_end_dt"))
    val statistic = spark.read
      .option("header", "true") // Если первая строка - заголовок
      .option("inferSchema", "true") // Автоматическое определение типов данных
      .option("delimiter", ";") // Разделитель, по умолчанию - запята
      .csv("src/main/resources/logs/display_run_log_statistic.csv")
      .withColumn("entity_name",expr("wf_nme_unq||'__'||tsk_nme"))
      .withColumn("rn",expr("row_number()").over(w))
      .where("rn = 1 and rls_stus = 'SUCCEEDED'")
      .select("entity_name",
        "rls_end_dt",
        "rls_stus",
        "wf_id"
      )


    val src = spark.read
      .option("header", "true") // Если первая строка - заголовок
      .option("inferSchema", "true") // Автоматическое определение типов данных
      .option("delimiter", ";") // Разделитель, по умолчанию - запята
      .csv("src/main/resources/logs/source.csv")

    val link_param = parent_list.as[Dependency]

    val obj_dependency = Dependency(link_param)(spark)
//    obj_dependency.printSchema()
    //obj_dependency.where("tbl_trg in ('i$cam.v$soi_vodnal','i$cam.v$soi_akciz_1151095','i$cam.v$soi_lic_zhm')").show(false)

    val l = """i$cam.v$soi_vodnal,i$cam.v$soi_akciz_1151095,i$cam.v$soi_lic_zhm""".split(",").toList

    val listTbl = TableCalc(obj_dependency, l)(spark)

//    listTbl.printSchema()
//    listTbl.orderBy("tbl_calc_nme","dependency_lvl").show(false)
//    println(listTbl.count())

    val log_stat = wf.as("w").join(tsk.as("t"), Seq("wf_id"), joinType = "inner")
      .join(statistic.as("s"),col("s.entity_name") === col("t.tsk_alias"))

    val obj_dep = listTbl.as("l").join(log_stat.as("s"),Seq("entity_name"), joinType = "left")
      .withColumn("wf_nme", expr("split(s.entity_name,'__')[0]"))
      .withColumn("tsk_nme", expr("split(s.entity_name,'__')[1]"))
      .select(col("tbl_calc_nme"),col("wf_nme"),
        col("tsk_nme"),
        col("rls_end_dt").as("lst_saccess_dt"),
        col("rls_stus").as("tsk_status"),
        col("tbl_src").as("src_tbl_nme"),
        col("tbl_trg").as("trg)tbl_nme"),
        col("dependency_lvl"))

    obj_dep.orderBy("tbl_calc_nme","dependency_lvl").show(false)
    obj_dep.write
      .option("header", "true") // Если первая строка - заголовок
      .option("inferSchema", "true") // Автоматическое определение типов данных
      .option("delimiter", ";") // Разделитель, по умолчанию - запята
      .csv("src/main/resources/operation_list.csv")

    parent_list.unpersist()
  }
}