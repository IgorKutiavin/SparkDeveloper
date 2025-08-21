import org.apache.spark.sql.functions._
import org.apache.spark.sql._

val spark = SparkSession.builder()
  .appName("SparkML")
  .master("local[*]")
  .getOrCreate()

val df = spark.read
  .option("header", "true") // Если первая строка - заголовок
  .option("inferSchema", "true") // Автоматическое определение типов данных
  .option("delimiter", ",") // Разделитель, по умолчанию - запята
  .csv("/home/igor/Spark/Developer/Project-Igor/SparkDeveloper/SparkML/src/Data")

//df.show(10, truncate = false)
//
//df.printSchema()
//
//df.groupBy("species")
//  .count()
//  .select("species","count")
//  .show(10)

val numCol = df.dtypes
  .filter(p => p._2.equals("DoubleType") || p._2.equals("IntegerType"))
  .map(_._1)

df.select(numCol.map(col).toIndexedSeq: _*).summary().show

val dft = df.withColumn("target", when(col("species") === "Iris-setosa", 1)
  .when(col("species") === "Iris-virginica",2)
  .when(col("species") === "Iris-versicolor",3)
  .otherwise("0"))

dft.select("species","target").show(5, truncate = false)
