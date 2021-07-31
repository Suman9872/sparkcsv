name := "helloSpark"

version := "0.1"

scalaVersion := "2.13.6"

autoScalaLibrary := false

val sparkVersion ="3.0.0-previews2"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion

)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

LibraryDependencies ++= sparkDependencies ++ testDependencies