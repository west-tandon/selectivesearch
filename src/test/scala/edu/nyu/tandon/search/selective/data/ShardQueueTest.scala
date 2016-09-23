package edu.nyu.tandon.search.selective.data

import java.util.NoSuchElementException

import org.scalatest.FunSuite

/**
  * @author michal.siedlaczek@nyu.edu
  */
class ShardQueueTest extends FunSuite {

  test("queue") {
    // given
    val sq = ShardQueue.maxPayoffQueue(QueryData("x", List(
      Bin(0, 9, 9), Bin(0, 5, 5), Bin(0, 1.0, 1.0),
      Bin(1, 8, 8), Bin(1, 2, 2)
    )))

    // then
    assert(sq.toList === List(
      Bin(0, 9, 9),
      Bin(1, 8, 8),
      Bin(0, 5, 5),
      Bin(1, 2, 2),
      Bin(0, 1, 1)))

    intercept[NoSuchElementException] {
      sq.dequeue()
    }
  }

}
