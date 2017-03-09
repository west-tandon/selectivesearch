package edu.nyu.tandon.utils

import scala.language.implicitConversions

/**
  * @author michal.siedlaczek@nyu.edu
  */
class Tuple2Iterator[A1, A2](val iterator: Iterator[(A1, A2)]) {

  def flatZip[B](other: Iterator[B]): Iterator[(A1, A2, B)] = new Iterator[(A1, A2, B)] {
    val zipped = iterator.zip(other)
    override def hasNext: Boolean = zipped.hasNext
    override def next(): (A1, A2, B) = zipped.next() match {
      case ((a1, a2), b) => (a1, a2, b)
    }
  }

  def flatZipWithIndex: Iterator[(A1, A2, Int)] = new Iterator[(A1, A2, Int)] {
    val zipped = iterator.zipWithIndex
    override def hasNext: Boolean = zipped.hasNext
    override def next(): (A1, A2, Int) = zipped.next() match {
      case ((a1, a2), index) => (a1, a2, index)
    }
  }

}

class Tuple3Iterator[A1, A2, A3](val iterator: Iterator[(A1, A2, A3)]) {

  def flatZip[B](other: Iterator[B]): Iterator[(A1, A2, A3, B)] = new Iterator[(A1, A2, A3, B)] {
    val zipped = iterator.zip(other)
    override def hasNext: Boolean = zipped.hasNext
    override def next(): (A1, A2, A3, B) = zipped.next() match {
      case ((a1, a2, a3), b) => (a1, a2, a3, b)
    }
  }

  def flatZipWithIndex: Iterator[(A1, A2, A3, Int)] = new Iterator[(A1, A2, A3, Int)] {
    val zipped = iterator.zipWithIndex
    override def hasNext: Boolean = zipped.hasNext
    override def next(): (A1, A2, A3, Int) = zipped.next() match {
      case ((a1, a2, a3), index) => (a1, a2, a3, index)
    }
  }

}

class Tuple4Iterator[A1, A2, A3, A4](val iterator: Iterator[(A1, A2, A3, A4)]) {

  def flatZip[B](other: Iterator[B]): Iterator[(A1, A2, A3, A4, B)] = new Iterator[(A1, A2, A3, A4, B)] {
    val zipped = iterator.zip(other)
    override def hasNext: Boolean = zipped.hasNext
    override def next(): (A1, A2, A3, A4, B) = zipped.next() match {
      case ((a1, a2, a3, a4), b) => (a1, a2, a3, a4, b)
    }
  }

  def flatZipWithIndex: Iterator[(A1, A2, A3, A4, Int)] = new Iterator[(A1, A2, A3, A4, Int)] {
    val zipped = iterator.zipWithIndex
    override def hasNext: Boolean = zipped.hasNext
    override def next(): (A1, A2, A3, A4, Int) = zipped.next() match {
      case ((a1, a2, a3, a4), index) => (a1, a2, a3, a4, index)
    }
  }

}

class Tuple5Iterator[A1, A2, A3, A4, A5](val iterator: Iterator[(A1, A2, A3, A4, A5)]) {

  def flatZip[B](other: Iterator[B]): Iterator[(A1, A2, A3, A4, A5, B)] = new Iterator[(A1, A2, A3, A4, A5, B)] {
    val zipped = iterator.zip(other)
    override def hasNext: Boolean = zipped.hasNext
    override def next(): (A1, A2, A3, A4, A5, B) = zipped.next() match {
      case ((a1, a2, a3, a4, a5), b) => (a1, a2, a3, a4, a5, b)
    }
  }

  def flatZipWithIndex: Iterator[(A1, A2, A3, A4, A5, Int)] = new Iterator[(A1, A2, A3, A4, A5, Int)] {
    val zipped = iterator.zipWithIndex
    override def hasNext: Boolean = zipped.hasNext
    override def next(): (A1, A2, A3, A4, A5, Int) = zipped.next() match {
      case ((a1, a2, a3, a4, a5), index) => (a1, a2, a3, a4, a5, index)
    }
  }

}

class Tuple6Iterator[A1, A2, A3, A4, A5, A6](val iterator: Iterator[(A1, A2, A3, A4, A5, A6)]) {

  def flatZip[B](other: Iterator[B]): Iterator[(A1, A2, A3, A4, A5, A6, B)] = new Iterator[(A1, A2, A3, A4, A5, A6, B)] {
    val zipped = iterator.zip(other)
    override def hasNext: Boolean = zipped.hasNext
    override def next(): (A1, A2, A3, A4, A5, A6, B) = zipped.next() match {
      case ((a1, a2, a3, a4, a5, a6), b) => (a1, a2, a3, a4, a5, a6, b)
    }
  }

  def flatZipWithIndex: Iterator[(A1, A2, A3, A4, A5, A6, Int)] = new Iterator[(A1, A2, A3, A4, A5, A6, Int)] {
    val zipped = iterator.zipWithIndex
    override def hasNext: Boolean = zipped.hasNext
    override def next(): (A1, A2, A3, A4, A5, A6, Int) = zipped.next() match {
      case ((a1, a2, a3, a4, a5, a6), index) => (a1, a2, a3, a4, a5, a6, index)
    }
  }

}

class Tuple7Iterator[A1, A2, A3, A4, A5, A6, A7](val iterator: Iterator[(A1, A2, A3, A4, A5, A6, A7)]) {

  def flatZip[B](other: Iterator[B]): Iterator[(A1, A2, A3, A4, A5, A6, A7, B)] = new Iterator[(A1, A2, A3, A4, A5, A6, A7, B)] {
    val zipped = iterator.zip(other)
    override def hasNext: Boolean = zipped.hasNext
    override def next(): (A1, A2, A3, A4, A5, A6, A7, B) = zipped.next() match {
      case ((a1, a2, a3, a4, a5, a6, a7), b) => (a1, a2, a3, a4, a5, a6, a7, b)
    }
  }

  def flatZipWithIndex: Iterator[(A1, A2, A3, A4, A5, A6, A7, Int)] = new Iterator[(A1, A2, A3, A4, A5, A6, A7, Int)] {
    val zipped = iterator.zipWithIndex
    override def hasNext: Boolean = zipped.hasNext
    override def next(): (A1, A2, A3, A4, A5, A6, A7, Int) = zipped.next() match {
      case ((a1, a2, a3, a4, a5, a6, a7), index) => (a1, a2, a3, a4, a5, a6, a7, index)
    }
  }

}

object TupleIterators {
  implicit def iterator2Tuple2Iterator[A1, A2](iterator: Iterator[(A1, A2)]): Tuple2Iterator[A1, A2] = new Tuple2Iterator[A1, A2](iterator)
  implicit def iterator2Tuple3Iterator[A1, A2, A3](iterator: Iterator[(A1, A2, A3)]): Tuple3Iterator[A1, A2, A3] = new Tuple3Iterator[A1, A2, A3](iterator)
  implicit def iterator2Tuple4Iterator[A1, A2, A3, A4](iterator: Iterator[(A1, A2, A3, A4)]): Tuple4Iterator[A1, A2, A3, A4] = new Tuple4Iterator[A1, A2, A3, A4](iterator)
  implicit def iterator2Tuple5Iterator[A1, A2, A3, A4, A5](iterator: Iterator[(A1, A2, A3, A4, A5)]): Tuple5Iterator[A1, A2, A3, A4, A5] = new Tuple5Iterator[A1, A2, A3, A4, A5](iterator)
  implicit def iterator2Tuple6Iterator[A1, A2, A3, A4, A5, A6](iterator: Iterator[(A1, A2, A3, A4, A5, A6)]): Tuple6Iterator[A1, A2, A3, A4, A5, A6] = new Tuple6Iterator[A1, A2, A3, A4, A5, A6](iterator)
  implicit def iterator2Tuple7Iterator[A1, A2, A3, A4, A5, A6, A7](iterator: Iterator[(A1, A2, A3, A4, A5, A6, A7)]): Tuple7Iterator[A1, A2, A3, A4, A5, A6, A7] = new Tuple7Iterator[A1, A2, A3, A4, A5, A6, A7](iterator)
}
