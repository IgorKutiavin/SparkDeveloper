package ru.kiv.spark

import SparkDataFrameAPI._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import ru.kiv.spark.SparkSessionCreationTest

class DataFrameAPITest
  extends AnyFlatSpec
  with SparkSessionCreationTest
  with Logging
  with BeforeAndAfter
  with DataFrameComparer {

  var testDf: DataFrame = _
  var countryDf: DataFrame = _

  before(
    testDf = spark.read
      .format("json")
      .option("mode", "FAILFAST")
      .option("multiline", value = true)
      .load("src/main/test/resources/countries.json")
  )

  it should "print testDf schema and show" in {
    testDf.printSchema
    testDf.show
  }

  it should "contain all necessary columns" in {
    val necessaryCols = Seq("Country", "borders")
    countryDf = testDf.withColumn("Country",col("name.common"))
      .select("Country","borders")
    assert(necessaryCols.forall(countryDf.columns.contains))
  }

  it should "return correct interaction list" in {

    import spark.implicits._

    val expectedDf =
      Seq("Russia", "United States", "China").toDF("Country")

    val resultDf = CountryReader(testDf).select("Country").as[String].filter( c => {c == "Russia" || c == "United States" || c == "China"}).toDF()

    assertSmallDatasetEquality(expectedDf, resultDf, orderedComparison = false)
  }
}