import scala.collection.Seq

ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.12.18"

val sparkVersion = "3.5.5"

ThisBuild / libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)

lazy val root = (project in file("."))
  .settings( name := "SparkDataSetAPI")
