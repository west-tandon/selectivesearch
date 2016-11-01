package edu.nyu.tandon.utils

import java.io.{BufferedReader, FileReader}

import edu.nyu.tandon.search.selective._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class ReadLineIterator(file: String) extends Iterator[String] {

  lazy val reader = new BufferedReader(new FileReader(file))

  var nextLine = reader.readLine()
  var closed = false

  override def hasNext: Boolean = nextLine != null
  override def next(): String = {
    val result = nextLine
    nextLine = reader.readLine()
    if (nextLine == null && !closed) {
      reader.close()
      closed = true
    }
    result
  }

  def of[A](implicit converter: String => A): Iterator[A] = this.map(converter)
  def ofSeq: Iterator[Seq[String]] = this.map(_.split(FieldSplitter).filter(_.length > 0))
  def ofSeq[A](implicit converter: String => A): Iterator[Seq[A]] = ofSeq.map(_.map(converter))

}

object Lines {
  def fromFile(file: String): ReadLineIterator = new ReadLineIterator(file)

  implicit val intConverter: String => Int = java.lang.Integer.parseInt
  implicit val longConverter: String => Long = java.lang.Long.parseLong
  implicit val doubleConverter: String => Double = java.lang.Double.parseDouble
}
