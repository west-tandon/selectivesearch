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

  def base(nestedBasename: String): String = nestedBasename.takeWhile(c => s"$c" != NestingIndicator)

  def loadProperties(basename: String): Properties = {
    val properties = new Properties()
    properties.load(new FileInputStream(s"${base(basename)}$PropertiesSuffix"))
    properties
  }

//  implicit def toClosingSource(source: Source) = new {
//    val lines = source.getLines()
//    var stillOpen = true
//    def getLinesAndClose() = new Iterator[String] {
//      def hasNext = stillOpen && lines.hasNext
//      def next = {
//        val line = lines.next
//        if (!lines.hasNext) { source.close() ; stillOpen = false }
//        line
//      }
//    }
//  }

  def lineToLongs(line: String): Seq[Long] = line.split(FieldSplitter).filter(_.length > 0).toSeq.map(_.toLong)
  def lineToDoubles(line: String): Seq[Double] = line.split(FieldSplitter).filter(_.length > 0).toSeq.map(_.toDouble)
  def lines[T](file: String)(implicit converter: String => T): Iterator[T] = {
//    val lines = new Iterator[T] {
//      val reader = new BufferedReader(new FileReader(file))
//      var nextLine = reader.readLine()
//      var closed = false
//      override def hasNext: Boolean = nextLine != null
//      override def next(): T = {
//        val result = nextLine
//        nextLine = reader.readLine()
//        if (nextLine == null && !closed) {
//          reader.close()
//          closed = true
//        }
//        converter(result)
//      }
//    }
//    val x = lines.next()
//    require(lines.nonEmpty, s"the file $file is empty")
    new LineIterator(file).map(converter)
  }

}
