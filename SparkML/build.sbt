ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.12.18"

lazy val sparkVersion = "3.5.5"

lazy val root = (project in file("."))
  .settings(
    name := "SparkML"
  )

libraryDependencies ++= Seq(
  "com.typesafe"      % "config"                    % "1.4.2",
  "org.apache.spark" %% "spark-sql"                 % sparkVersion % "provided",
  "org.apache.spark"  % "spark-mllib_2.12"          % sparkVersion % "provided",
)