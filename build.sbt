name := "selectivesearch"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.github.scopt" %% "scopt" % "3.5.0",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "test",
  "commons-io" % "commons-io" % "2.5",
  "com.github.scala-incubator.io" %% "scala-io-core" % "0.4.3-1"
)