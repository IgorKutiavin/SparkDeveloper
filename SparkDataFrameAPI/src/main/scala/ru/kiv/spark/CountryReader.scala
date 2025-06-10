package ru.kiv.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import ru.kiv.spark.SparkDataFrameAPI.spark

import scala.::

case class CountryReader (Country: String, borders: Array[String])

object CountryReader {

  import spark.implicits._

  def apply(df: DataFrame): DataFrame = {
    val ndf = df.withColumn("Country",col("name.common"))
      .select("Country","borders")

    val countryDF = ndf.as[CountryReader]
      .filter(country => country.borders.length >= 5)
      .map(x => (x.Country, x.borders.length, x.borders.mkString(",")))
      .withColumn("Country",col("_1"))
      .withColumn("NumBorder",col("_2"))
      .withColumn("BorderCountries",col("_3"))
      .select("Country","NumBorder","BorderCountries")

    countryDF
  }


}
