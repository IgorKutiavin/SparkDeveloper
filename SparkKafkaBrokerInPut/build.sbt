ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "SparkKafkaBrokerInPut"
  )

lazy val sparkVersion = "3.5.5"
lazy val kafkaVersion = "3.5.0"

libraryDependencies ++= Seq(
  "com.typesafe"      % "config"                    % "1.4.2",
  "org.apache.spark" %% "spark-sql"                 % sparkVersion % "provided",
  "org.apache.spark"  % "spark-mllib_2.12"          % sparkVersion % "provided",
  "org.apache.spark"  % "spark-sql-kafka-0-10_2.12" % sparkVersion,
  "org.apache.kafka"  % "kafka-clients"             % kafkaVersion,
  "org.slf4j"         %"slf4j-simple"               % "1.6.1"
)