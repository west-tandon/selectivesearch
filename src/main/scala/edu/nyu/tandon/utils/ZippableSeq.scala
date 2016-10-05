package edu.nyu.tandon.utils

import scala.language.implicitConversions

/**
  * @author michal.siedlaczek@nyu.edu
  */
class ZippableSeq[T](val innerSeq: Seq[Iterable[T]]) {

  implicit def seq2Expanded(s: Seq[Iterable[T]]): ZippableSeq[T] = new ZippableSeq[T](s)
  implicit def indexedSeq2Expanded(s: IndexedSeq[Iterable[T]]): ZippableSeq[T] = new ZippableSeq[T](s)

  def zipped: Iterable[Seq[T]] = new Iterable[Seq[T]] {
    override def iterator: Iterator[Seq[T]] = new BulkIterator[T](innerSeq.map(_.iterator))
  }

}
