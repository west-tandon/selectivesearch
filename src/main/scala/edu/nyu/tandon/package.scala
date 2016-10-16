package edu.nyu

import java.io.{BufferedReader, FileInputStream, FileReader}
import java.util.Properties

import edu.nyu.tandon.search.selective._
import edu.nyu.tandon.utils.LineIterator

import scala.io.Source

/**
  * @author michal.siedlaczek@nyu.edu
  */
package object tandon {

  val PropertiesSuffix = ".properties"

  def defaultConverter(line: String) = line

//  def base(nestedBasename: String): String = nestedBasename.takeWhile(c => s"$c" != NestingIndicator)

  def loadProperties(basename: String): Properties = {
    val properties = new Properties()
    properties.load(new FileInputStream(s"${base(basename)}$PropertiesSuffix"))
    properties
  }

  def lineToLongs(line: String): Seq[Long] = line.split(FieldSplitter).filter(_.length > 0).toSeq.map(_.toLong)
  def lineToDoubles(line: String): Seq[Double] = line.split(FieldSplitter).filter(_.length > 0).toSeq.map(_.toDouble)
  def lines[T](file: String)(implicit converter: String => T): Iterator[T] = {
    new LineIterator(file).map(converter)
  }

}
