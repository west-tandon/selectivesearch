package edu.nyu.tandon.search.selective.verbose

import java.io.{BufferedWriter, StringWriter}

import edu.nyu.tandon.test.BaseFunSuite
import org.scalatest.Matchers._

import scala.collection.mutable

/**
  * @author michal.siedlaczek@nyu.edu
  */
class VerboseSelectorTest extends BaseFunSuite {

  trait Queue {
    val k = 4
    val q = new mutable.PriorityQueue[Result]()(VerboseSelector.scoreOrdering)
    q.enqueue(
      Result(1.0, relevant = true, originalRank = 4),
      Result(0.9, relevant = false, originalRank = 11),
      Result(0.8, relevant = false, originalRank = 5),
      Result(0.7, relevant = false, originalRank = 12)
    )
  }

  trait Shards {
    val s0 = new Shard(id = 0, List(
      Bucket(shardId = 0, List(Result(1.0, relevant = true, originalRank = 4)), impact = 1.0, cost = 1.0, postings = 10), // already selected
      Bucket(shardId = 0, List(Result(0.9, relevant = false, originalRank = 20)), impact = 0.9, cost = 1.0, postings = 10),
      Bucket(shardId = 0, List(Result(0.5, relevant = true, originalRank = 3)), impact = 0.5, cost = 1.0, postings = 10),
      Bucket(shardId = 0, List(Result(0.3, relevant = true, originalRank = 6)), impact = 0.3, cost = 1.0, postings = 10)
    ), numSelected = 1)
    val s1 = new Shard(id = 1, List(
      Bucket(shardId = 1, List(Result(1.0, relevant = true, originalRank = 4)), impact = 1.0, cost = 1.0, postings = 10), // already selected
      Bucket(shardId = 1, List(Result(0.8, relevant = true, originalRank = 0)), impact = 0.8, cost = 1.0, postings = 10),
      Bucket(shardId = 1, List(Result(0.7, relevant = false, originalRank = 13)), impact = 0.7, cost = 1.0, postings = 10),
      Bucket(shardId = 1, List(Result(0.1, relevant = false, originalRank = 10)), impact = 0.1, cost = 1.0, postings = 10)
    ), numSelected = 1)
  }

  trait Selector extends Shards with Queue {
    val selector = new VerboseSelector(Seq(s0, s1), q, 0)
  }

  test("result ordering") {
    val top = new mutable.PriorityQueue[Result]()(VerboseSelector.scoreOrdering)
    top.enqueue(
      Result(0.5, relevant = true, originalRank = 0),
      Result(2.0, relevant = true, originalRank = 1),
      Result(1.0, relevant = true, originalRank = 2)
    )
    top.dequeueAll.map(_.score) should contain theSameElementsInOrderAs Seq(2.0, 1.0, 0.5)
  }

  test("lastSelectedBucket") {
    new Selector {
      selector.lastSelectedBucket shouldBe 0
    }
  }

  test("precisionAt") {
    new Selector {
      selector.precisionAt(1) shouldBe 1.0
      selector.precisionAt(2) shouldBe 0.5
      selector.precisionAt(4) shouldBe 0.25
    }
  }

  test("overlapAt") {
    new Selector {
      selector.overlapAt(3) shouldBe 0.0
      selector.overlapAt(4) shouldBe 0.25
      selector.overlapAt(10) shouldBe 0.2
    }
  }

  test("numRelevantInLastSelected") {
    new Selector {
      selector.numRelevantInLastSelected() shouldBe 1
    }
  }

  test("numTopInLastSelected") {
    new Selector {
      selector.numTopInLastSelected(3) shouldBe 0
      selector.numTopInLastSelected(4) shouldBe 1
      selector.numTopInLastSelected(10) shouldBe 1
    }
  }

  test("selectNext") {
    new Selector {
      var next = selector.selectNext().get
      next.lastSelectedShard shouldBe 0
      next.lastSelectedBucket shouldBe 1

      next = next.selectNext().get
      next.lastSelectedShard shouldBe 1
      next.lastSelectedBucket shouldBe 1

      next = next.selectNext().get
      next.lastSelectedShard shouldBe 1
      next.lastSelectedBucket shouldBe 2

      next = next.selectNext().get
      next.lastSelectedShard shouldBe 0
      next.lastSelectedBucket shouldBe 2

      next = next.selectNext().get
      next.lastSelectedShard shouldBe 0
      next.lastSelectedBucket shouldBe 3

      next = next.selectNext().get
      next.lastSelectedShard shouldBe 1
      next.lastSelectedBucket shouldBe 3

      next.selectNext() shouldBe None
    }
  }

  test("processSelector") {
    new Selector {

      // given
      val strWriter = new StringWriter()
      val writer = new BufferedWriter(strWriter)
      val precisions = Seq(10, 30)
      val overlaps = Seq(10, 30)

      // when
      VerboseSelector.printHeader(precisions, overlaps)(writer)
      VerboseSelector.processSelector(precisions, overlaps, 2)(0, selector, writer)

      strWriter.toString shouldBe Seq(
        "qid,step,cost,postings,postings_relative,P@10,P@30,O@10,O@30,last_shard,last_bucket,last_cost,last_postings,last#relevant,last#top_10,last#top_30\n",
        "0,1,1.0,10,0.125,0.1,0.0333,0.2,0.1667,0,1,1.0,10,1,1,2\n",
        "0,2,2.0,20,0.25,0.2,0.0667,0.3,0.2,1,1,1.0,10,2,2,2\n",
        "0,3,3.0,30,0.375,0.2,0.0667,0.3,0.2333,1,2,1.0,10,2,2,3\n",
        "0,4,4.0,40,0.5,0.3,0.1,0.4,0.2667,0,2,1.0,10,2,2,3\n",
        "0,5,5.0,50,0.625,0.4,0.1333,0.5,0.3,0,3,1.0,10,3,3,4\n",
        "0,6,6.0,60,0.75,0.4,0.1333,0.6,0.3333,1,3,1.0,10,2,3,4\n"
      ).mkString
    }
  }

}
