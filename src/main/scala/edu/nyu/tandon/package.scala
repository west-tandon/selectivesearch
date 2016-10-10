package edu.nyu

import java.io.FileInputStream
import java.util.Properties

import edu.nyu.tandon.search.selective._
import edu.nyu.tandon.search.selective.learn.LearnPayoffs
import org.apache.spark.sql.SparkSession

import scalax.io.{LongTraversable, Resource}

/**
  * @author michal.siedlaczek@nyu.edu
  */
package object tandon {

  val PropertiesSuffix = ".properties"

  def defaultConverter(line: String) = line

  def base(nestedBasename: String): String = nestedBasename.takeWhile(c => s"$c" != NestingIndicator)

  def loadProperties(basename: String): Properties = {
    val properties = new Properties()
    properties.load(new FileInputStream(s"${base(basename)}$PropertiesSuffix"))
    properties
  }

  def lineToLongs(line: String): Seq[Long] = line.split(FieldSplitter).filter(_.length > 0).toSeq.map(_.toLong)
  def lineToDoubles(line: String): Seq[Double] = line.split(FieldSplitter).filter(_.length > 0).toSeq.map(_.toDouble)
  def lines[T](file: String)(implicit converter: String => T): LongTraversable[T] = {
    val lines = Resource.fromFile(file).lines()
    require(lines.nonEmpty, s"the file $file is either empty or doesn't exist")
    lines.map(converter)
  }

}
