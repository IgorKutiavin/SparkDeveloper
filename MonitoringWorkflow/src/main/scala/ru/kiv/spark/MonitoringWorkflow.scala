package ru.kiv.spark

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.typesafe.config.ConfigFactory

object MonitoringWorkflow {

  def main(args: Array[String]): Unit = {

    val config             = ConfigFactory.load()
    val operation_list     = config.getString("input.operation_list")
    val workflow           = config.getString("input.workflow")
    val task               = config.getString("input.task")
    val log_statistics     = config.getString("input.log_statistics")
    val src_list           = config.getString("input.src_list")
    val out_operation_list = config.getString("output.out_operation_list")

    val spark = SparkSession.builder()
      .appName("MonitoringWorkflow")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val parent_list = spark.read
      .option("header", "true") // Если первая строка - заголовок
      .option("inferSchema", "true") // Автоматическое определение типов данных
      .option("delimiter", ";") // Разделитель, по умолчанию - запята
      .csv(operation_list)
      .persist()

    val wf = spark.read
      .option("header", "true") // Если первая строка - заголовок
      .option("inferSchema", "true") // Автоматическое определение типов данных
      .option("delimiter", ";") // Разделитель, по умолчанию - запята
      .csv(workflow)


    val tsk = spark.read
      .option("header", "true") // Если первая строка - заголовок
      .option("inferSchema", "true") // Автоматическое определение типов данных
      .option("delimiter", ";") // Разделитель, по умолчанию - запята
      .csv(task)


    val w = Window.partitionBy("wf_nme_unq","tsk_nme","rls_stus").orderBy(desc("rls_end_dt"))
    val statistic = spark.read
      .option("header", "true") // Если первая строка - заголовок
      .option("inferSchema", "true") // Автоматическое определение типов данных
      .option("delimiter", ";") // Разделитель, по умолчанию - запята
      .csv(log_statistics)
      .withColumn("entity_name",expr("wf_nme_unq||'__'||tsk_nme"))
      .withColumn("rn",expr("row_number()").over(w))
      .where("rn = 1 and rls_stus = 'SUCCEEDED'")
      .select("entity_name",
        "rls_end_dt",
        "rls_stus",
        "wf_id"
      )

    val src = spark.read
      .option("inferSchema", "true") // Автоматическое определение типов данных
      .option("delimiter", ";") // Разделитель, по умолчанию - запята
      .csv(src_list)
      .as[String].take(3).toList

    val link_param = parent_list.as[Dependency]

    val obj_dependency = Dependency(link_param)(spark)

    val listTbl = TableCalc(obj_dependency, src)(spark)

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
        col("tbl_trg").as("trg_tbl_nme"),
        col("dependency_lvl")).distinct()

    //obj_dep.orderBy("tbl_calc_nme","dependency_lvl").show(false)
    //obj_dep.select("tbl_calc_nme","dependency_lvl").distinct().orderBy("tbl_calc_nme","dependency_lvl").show(false)

    obj_dep.write
      .mode("overwrite")
      .option("header", "true") // Если первая строка - заголовок
      .option("inferSchema", "true") // Автоматическое определение типов данных
      .option("delimiter", ";") // Разделитель, по умолчанию - запята
      .csv(out_operation_list)

    parent_list.unpersist()
  }
}