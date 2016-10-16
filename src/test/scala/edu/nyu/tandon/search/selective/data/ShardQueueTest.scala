package edu.nyu.tandon.search.selective.data

import java.util.NoSuchElementException

import org.scalatest.FunSuite

/**
  * @author michal.siedlaczek@nyu.edu
  */
class ShardQueueTest extends FunSuite {

  test("queue") {
    // given
    val sq = ShardQueue.maxPayoffQueue(QueryData(Seq(
      List(Bucket(0, 9, 9), Bucket(0, 5, 5), Bucket(0, 1.0, 1.0)),
      List(Bucket(1, 8, 8), Bucket(1, 2, 2))
    )))

    // then
    assert(sq.toList === List(
      Bucket(0, 9, 9),
      Bucket(1, 8, 8),
      Bucket(0, 5, 5),
      Bucket(1, 2, 2),
      Bucket(0, 1, 1)))

    intercept[NoSuchElementException] {
      sq.dequeue()
    }
  }

}
