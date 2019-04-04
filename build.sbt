
name := "scala_example"

version := "0.1"

scalaVersion := "2.11.12"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "joda-time" % "joda-time" % "2.10.1",
  "mysql" % "mysql-connector-java" % "5.1.46"
)
