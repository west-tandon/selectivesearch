package edu.nyu.tandon.search.selective.data

import scala.collection.mutable

/**
  * @author michal.siedlaczek@nyu.edu
  */
class ShardQueue(pq: mutable.PriorityQueue[List[Bin]])
  extends mutable.AbstractIterable[Bin] {

  def dequeue(): Bin = {
    pq.dequeue() match {
      case Nil =>
        throw new NoSuchElementException("no element to remove from the queue");
      case head :: Nil =>
        head
      case head :: tail =>
        pq.enqueue(tail)
        head
    }
  }

  override def iterator: Iterator[Bin] = new Iterator[Bin] {
    override def hasNext: Boolean = pq.nonEmpty
    override def next(): Bin = dequeue()
  }

}

object ShardQueue {

  def maxPayoffQueue(queryData: QueryData): ShardQueue = {
    val queue = new mutable.PriorityQueue[List[Bin]]()(Ordering.by(_.head.payoff))
    queryData.binsByShard
      .filter(_.nonEmpty)
      .foreach(queue.enqueue(_))
    new ShardQueue(queue)
  }

}
