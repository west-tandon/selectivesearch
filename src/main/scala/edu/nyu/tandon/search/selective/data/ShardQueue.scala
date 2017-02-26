package edu.nyu.tandon.search.selective.data

import scala.collection.mutable

/**
  * @author michal.siedlaczek@nyu.edu
  */
class ShardQueue(pq: mutable.PriorityQueue[List[Bucket]])
  extends mutable.AbstractIterable[Bucket] {

  def dequeue(): Bucket = {
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

  override def iterator: Iterator[Bucket] = new Iterator[Bucket] {
    override def hasNext: Boolean = pq.nonEmpty
    override def next(): Bucket = dequeue()
  }

}

object ShardQueue {

  def maxPayoffQueue(queryData: QueryData): ShardQueue = {
    val queue = new mutable.PriorityQueue[List[Bucket]]()(Ordering.by((list) =>
      list.head.payoff / (list.head.cost + list.head.penalty)))
    queryData.bucketsByShard
      .filter(_.nonEmpty)
      .foreach(queue.enqueue(_))
    new ShardQueue(queue)
  }

}
