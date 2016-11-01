package edu.nyu.tandon.utils

import java.io.FileWriter

import scala.language.implicitConversions

/**
  * @author michal.siedlaczek@nyu.edu
  */
class WriteLineIterator[A](val iterator: Iterator[A]) extends Iterator[A] {
  override def hasNext: Boolean = iterator.hasNext
  override def next(): A = iterator.next()
  def write(file: String): Unit = {
    val writer = new FileWriter(file)
    for (x <- iterator) writer.append(s"$x\n")
    writer.close()
  }
  def aggregating(implicit toDouble: A => Double): AggregatingWriteLineIterator[A] =
    new AggregatingWriteLineIterator[A](iterator, toDouble)
}

class AggregatingWriteLineIterator[A](val iterator: Iterator[A], val converter: A => Double) extends Iterator[A] {
  override def hasNext: Boolean = iterator.hasNext
  override def next(): A = iterator.next()
  def write(file: String): (Double, Int) = {
    val writer = new FileWriter(file)
    var sum: Double = 0
    var count = 0
    for (x <- iterator) {
      writer.append(s"$x\n")
      sum += converter(x)
      count += 1
    }
    writer.close()
    (sum, count)
  }
}

object WriteLineIterator {
  implicit def iterator2WriteIterator[A](iterator: Iterator[A]): WriteLineIterator[A] = new WriteLineIterator[A](iterator)
}
