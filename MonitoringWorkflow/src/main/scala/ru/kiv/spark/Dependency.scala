package ru.kiv.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class Dependency (entity_name: String,
                      param_val: String,
                      entity_typ: String,
                      param_typ: String,
                      param_lnk_typ_nme: String)

object Dependency {
//  def apply (entity_name: String, param_val: String, entity_typ: String, param_typ: String, param_lnk_typ_nme: String): Dependency = {
//    Dependency( entity_name,
//      param_val,
//      entity_typ,
//      param_typ,
//      param_lnk_typ_nme
//    )
//  }
//
//
  def obj_depend (ds: Dataset[Dependency])(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val obj_dependency_trg = ds
      .filter(p => p.entity_typ == "TASK"
        && p.param_lnk_typ_nme == "TRG"
        && (p.param_typ match {
        case "TABLE" => true
        case "TABLE_NAME" => true
        case "ENTITY" => true
        case _ => false
      })).map(p => {
        val tbl_trg = parse_tbl(p.param_val)
        val param_val_trg = p.param_val.toLowerCase()
        val tbl_trg_host = p.param_val.toLowerCase().split("__")(0)

        (p.entity_name,tbl_trg,param_val_trg,tbl_trg_host)
      }
      ).select(col("_1").as("entity_name"),col("_2").as("tbl_trg"),col("_3").as("param_val_trg"),col("_4").as("tbl_trg_host"))

    val obj_dependency_src = ds
      .filter(p => p.entity_typ == "TASK"
        && p.param_lnk_typ_nme == "SRC"
        && (p.param_typ match {
        case "TABLE" => true
        case "TABLE_NAME" => true
        case "ENTITY" => true
        case _ => false
      })).map(p => {
        val tbl_src = parse_tbl(p.param_val)
        val param_val_src = p.param_val.toLowerCase()

        (p.entity_name,tbl_src,param_val_src)
      }).select(col("_1").as("entity_name"),col("_2").as("tbl_src"),col("_3").as("param_val_src"))

    obj_dependency_trg.as("o")
      .join(obj_dependency_src.as("d"),Seq("entity_name"),joinType = "inner")
      .select("o.entity_name",
      "tbl_trg",
      "tbl_src",
      "param_val_trg",
      "param_val_src",
      "tbl_trg_host"
      )
  }

  def parse_tbl (p: String):String = {
    import scala.util.matching.Regex

    val pr:Regex = """.*__([0-9a-zA-Z-_]+)__([0-9a-zA-Z-_]+)""".r

    p match{
      case pr(sh,tbl) => s"$sh.$tbl"
      case _ => ""
    }
  }
}