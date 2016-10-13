package edu.nyu.tandon.utils

/**
  * @author michal.siedlaczek@nyu.edu
  */
class IteratorSeq[T](val iterators: Iterator[Seq[T]]) extends Seq[Iterator[T]] {
  override def length: Int = ???

  override def apply(idx: Int): Iterator[T] = ???

  override def iterator: Iterator[Iterator[T]] = ???
}
