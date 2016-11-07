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
  def aggregating(implicit n: Numeric[A]): AggregatingWriteLineIterator[A] =
    new AggregatingWriteLineIterator[A](iterator, n)
}

class AggregatingWriteLineIterator[A](val iterator: Iterator[A], val n: Numeric[A]) extends Iterator[A] {
  override def hasNext: Boolean = iterator.hasNext
  override def next(): A = iterator.next()
  def write(file: String): (Double, Int) = {
    val writer = new FileWriter(file)
    var sum: Double = 0
    var count = 0
    for (x <- iterator) {
      writer.append(s"$x\n")
      sum += n.toDouble(x)
      count += 1
    }
    writer.close()
    (sum, count)
  }
}

object WriteLineIterator {
  implicit def iterator2WriteIterator[A](iterator: Iterator[A]): WriteLineIterator[A] = new WriteLineIterator[A](iterator)
  implicit def stream2WriteIterator[A](stream: Stream[A]): WriteLineIterator[A] = new WriteLineIterator[A](stream.iterator)
  implicit def seq2WriteIterator[A](seq: Seq[A]): WriteLineIterator[A] = new WriteLineIterator[A](seq.iterator)
}
