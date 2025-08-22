package ru.kiv.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.annotation.tailrec

case class TableCalc (tbl_calc_nme: String,
                      entity_name: String,
                      tbl_trg: String,
                      tbl_src: String,
                      param_val_trg: String,
                      param_val_src: String,
                      tbl_trg_host: String,
                     )

case class TableDepend(tbl_calc_nme: String,
                       entity_name: String,
                       tbl_trg: String,
                       tbl_src: String,
                       param_val_trg: String,
                       param_val_src: String,
                       tbl_trg_host: String,
                       dependency_lvl: Int
                      )

object TableCalc {

  def apply(df: DataFrame, listSrc: List[String])(implicit spark: SparkSession):DataFrame = {

    import spark.implicits._

    df.printSchema()

    val dep_lvl = 1
    val ds = df.withColumn("tbl_calc_nme", col("tbl_trg"))
    .as[TableCalc]

    val dep = df.withColumn("tbl_calc_nme", col("tbl_trg"))
      .withColumn("dependency_lvl", expr(s"$dep_lvl"))
      .as[TableDepend]
      .filter(x=> listSrc.contains(x.tbl_trg.toLowerCase))

    @tailrec
    def rollUp(in_ds: Dataset[TableCalc], acc_ds: Dataset[TableDepend], lvl: Int): DataFrame = {
        println(s"dependency_lvl: $lvl")
        val ds = in_ds.as("o").joinWith(acc_ds.as("p"), col("o.param_val_trg") === col("p.param_val_src"), joinType = "inner")
          .map(p => TableDepend(p._2.tbl_calc_nme, p._1.entity_name, p._1.tbl_trg, p._1.tbl_src, p._1.param_val_trg, p._1.param_val_src, p._1.tbl_trg_host, lvl))
        val ds_out = if (lvl == 1) {
          ds
        } else {
          acc_ds.union(ds)
        }

      if (lvl <= 5) {
        rollUp(in_ds, ds_out, lvl + 1)
      }
      else
        acc_ds.toDF()
    }

    rollUp(ds,dep,dep_lvl)
  }

}
