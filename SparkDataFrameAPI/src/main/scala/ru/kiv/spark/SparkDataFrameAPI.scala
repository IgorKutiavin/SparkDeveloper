package ru.kiv.spark

import org.apache.spark.sql.functions._

object SparkDataFrameAPI extends SparkSessionCreation {
  def main (args: Array[String]): Unit = {
    import spark.implicits._

    val df = spark.read
      .format("json")
      .option("multiline", "true")
      .option("mode","PERMISSIVE")
      .load(args(0))

    val rsDF = CountryReader(df)

    rsDF.write
      .format("parquet")
      .mode("overwrite")
      .save(args(1))

    val lrDF = LanguageReader(df)

    lrDF.write
      .format("parquet")
      .mode("overwrite")
      .save(args(2))
  }

}
