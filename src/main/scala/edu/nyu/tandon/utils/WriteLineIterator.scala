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
}

object WriteLineIterator {
  implicit def iterator2WriteIterator[A](iterator: Iterator[A]): WriteLineIterator[A] = new WriteLineIterator[A](iterator)
}
