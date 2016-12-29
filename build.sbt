name := "selectivesearch"

organization := "edu.nyu.tandon"

version := "1.0"

scalaVersion := "2.11.8"

enablePlugins(JavaAppPackaging)

mainClass in Compile := Some("edu.nyu.tandon.search.selective.Run")

libraryDependencies ++= Seq(
  "com.github.scopt" %% "scopt" % "3.5.0",
  "org.scalatest" %% "scalatest" % "2.2.5" % "test",
  "commons-io" % "commons-io" % "2.5",
  "com.github.scala-incubator.io" %% "scala-io-core" % "0.4.3",
  "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3",
  "org.apache.spark" %% "spark-core" % "2.0.0",
  "org.apache.spark" %% "spark-mllib" % "2.0.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
)