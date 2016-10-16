package edu.nyu.tandon.utils

/**
  * @author michal.siedlaczek@nyu.edu
  */
class BulkIterable[T](val iterables: Seq[Iterable[T]]) extends Iterable[Seq[T]] {
  override def iterator: Iterator[Seq[T]] = new ZippedIterator[T](iterables.map(_.iterator))
}
