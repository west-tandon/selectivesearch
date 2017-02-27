package edu.nyu.tandon.search.selective

import edu.nyu.tandon.takeUntilOrNil
import edu.nyu.tandon.test.BaseFunSuite
import edu.nyu.tandon.search.selective.clairvoyant.{Bucket, Result, Shard, ClairvoyantSelector}
import org.scalatest.Matchers._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class ClairvoyantSelectorTest extends BaseFunSuite {

  test("takeUntilOrNil") {
    val p: Int => Boolean = _ == 0
    takeUntilOrNil(Nil)(p) shouldBe Nil
    takeUntilOrNil(List(0))(p) should contain theSameElementsInOrderAs List(0)
    takeUntilOrNil(List(1))(p) shouldBe Nil
    takeUntilOrNil(List(1, 1, 1))(p) shouldBe Nil
    takeUntilOrNil(List(1, 0))(p) should contain theSameElementsInOrderAs List(1, 0)
    takeUntilOrNil(List(1, 1, 0))(p) should contain theSameElementsInOrderAs List(1, 1, 0)
    takeUntilOrNil(List(1, 0, 0))(p) should contain theSameElementsInOrderAs List(1, 0)
    takeUntilOrNil(List(1, 0, 1, 0))(p) should contain theSameElementsInOrderAs List(1, 0)
  }

  trait S {
    val s1 = new Shard(id = 0, List(
      Bucket(List(Result(1.0, hit = true)), 1.0), // already selected
      Bucket(List(Result(1.2, hit = false)), 1.0),
      Bucket(List(Result(1.3, hit = true)), 0.001),
      Bucket(List(Result(1.5, hit = true)), 0.001)
    ), numSelected = 1, costOfSelected = 1.0)
    val s2 = new Shard(id = 1, List(
      Bucket(List(Result(1.0, hit = false)), 1.0),
      Bucket(List(Result(1.2, hit = false)), 1.0),
      Bucket(List(Result(1.3, hit = false)), 0.001),
      Bucket(List(Result(1.5, hit = false)), 0.001)
    ))
    val s3 = new Shard(id = 2, List(
      Bucket(List(Result(0.9, hit = true)), 1.0), // already selected
      Bucket(List(Result(1.2, hit = true)), 1.0),
      Bucket(List(Result(1.3, hit = false)), 0.001),
      Bucket(List(Result(1.5, hit = false)), 0.001)
    ), numSelected = 1, costOfSelected = 1.0)
  }

  trait C extends S {
    val shards = List(
      Shard(id = 0, s1.buckets),
      s2,
      Shard(id = 2, s3.buckets)
    )
  }

  test("Shard.nextAvailable") {
    new S {
      val Some(shard1) = s1.nextAvailable
      shard1.numSelected shouldBe 3
      shard1.costOfSelected shouldBe 2.001

      val Some(shard2) = shard1.nextAvailable
      shard2.numSelected shouldBe 4
      shard2.costOfSelected shouldBe 2.002

      shard2.nextAvailable shouldBe None

      val Some(shard3) = s3.nextAvailable
      shard3.numSelected shouldBe 2
      shard3.costOfSelected shouldBe 2.0

      shard3.nextAvailable shouldBe None
    }
  }

  test("ClairvoyantSelector.overlap") {
    new S {
      ClairvoyantSelector(List(s1, s3), 0, 4).overlap shouldBe 0.5
      ClairvoyantSelector(List(Shard(id = 1, s1.buckets, 4, 2.002), s3), 0, 4).overlap shouldBe 0.75
    }
  }

  test("ClairvoyantSelector.select") {
    new C {
      {
        val selected = ClairvoyantSelector(shards, budget = 3.0, k = 4).select()
        selected.shards.map(_.numSelected) should contain theSameElementsInOrderAs List(1, 0, 2)
        selected.overlap shouldBe 0.75
      }
      {
        val selected = ClairvoyantSelector(shards, budget = 2.0, k = 4).select()
        selected.shards.map(_.numSelected) should contain theSameElementsInOrderAs List(0, 0, 2)
        selected.overlap shouldBe 0.5
      }
      {
        val selected = ClairvoyantSelector(shards, budget = 4.002, k = 6).select()
        selected.shards.map(_.numSelected) should contain theSameElementsInOrderAs List(4, 0, 2)
        selected.overlap shouldBe (0.833 +- 0.001)
      }
    }
  }
}
