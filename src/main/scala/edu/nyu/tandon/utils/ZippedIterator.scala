package edu.nyu.tandon.utils

/**
  * @author michal.siedlaczek@nyu.edu
  */
class ZippedIterator[T](val iterators: Seq[Iterator[T]]) extends Iterator[Seq[T]] {
  override def hasNext: Boolean = iterators map (_.hasNext) reduce (_ && _)
  override def next(): Seq[T] = for (i <- iterators) yield i.next()
  def strict: Iterator[Seq[T]] = new Iterator[Seq[T]] {
    override def hasNext: Boolean = {
      val allHaveNext = ZippedIterator.this.hasNext
      val noneHasNext = iterators map (!_.hasNext) reduce (_ && _)
      require(allHaveNext || noneHasNext, "strictness not met: some iterators have and some have not next")
      allHaveNext
    }
    override def next(): Seq[T] = ZippedIterator.this.next()
  }
}

object ZippedIterator {
  def apply[T](iterators: Seq[Iterator[T]]) = new ZippedIterator[T](iterators)
}