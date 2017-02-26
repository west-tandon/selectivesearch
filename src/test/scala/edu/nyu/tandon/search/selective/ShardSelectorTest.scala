package edu.nyu.tandon.search.selective

import edu.nyu.tandon.search.selective.data.{Bucket, QueryShardExperiment}
import edu.nyu.tandon.test.BaseFunSuite

import org.scalatest.Matchers._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class ShardSelectorTest extends BaseFunSuite {

  trait Selector {
    val queryExperiment = QueryShardExperiment.fromBasename(getClass.getResource("/").getPath + "test")
    val selector = new ShardSelector(queryExperiment, ShardSelector.bucketsWithinBudget(5), alpha = 1.0)
  }

  trait Buckets {
    val buckets = List(
      Bucket(0, 9, 9),
      Bucket(1, 8, 8),
      Bucket(0, 5, 5),
      Bucket(1, 2, 2),
      Bucket(0, 1, 1)
    )
  }

  test("bucketsUntilThreshold") {
    new Buckets {
      // given
      val threshold = 2.0

      // when
      val but = ShardSelector.bucketsUntilThreshold(threshold)(buckets)

      // then
      assert(but == Seq(Bucket(0, 9, 9), Bucket(1, 8, 8), Bucket(0, 5, 5)))
    }
  }

  test("bucketsWithinBudget") {
    new Buckets {
      // given
      val budget = 17.5

      // when
      val bwb = ShardSelector.bucketsWithinBudget(budget)(buckets)

      // then
      assert(bwb == Seq(Bucket(0, 9, 9), Bucket(1, 8, 8)))
    }
  }

  test("bucketsWithinBudget: budget even with the sum of costs") {
    new Buckets {
      // given
      val budget = 17

      // when
      val bwb = ShardSelector.bucketsWithinBudget(budget)(buckets)

      // then
      assert(bwb == Seq(Bucket(0, 9, 9), Bucket(1, 8, 8)))
    }
  }

  /*
   * Still should return the first bucket.
   */
  test("bucketsWithinBudget: budget lower than the first cost") {
    new Buckets {
      // given
      val budget = 5

      // when
      val bwb = ShardSelector.bucketsWithinBudget(budget)(buckets)

      // then
      assert(bwb == Seq(Bucket(0, 9, 9)))
    }
  }

  test("selector") {
    new Selector {
      // when
      val l = selector.toList

      // then
      assert(l === List(
        List(0, 3, 2),
        List(3, 0, 2)
      ))
    }
  }

  test("main: with scores") {
    // given
    val tmpDir = createTemporaryCopyOfResources(regex = ".*sizes|.*results.*|.*scores|.*properties|.*queries|.*payoff|.*cost")

    // when
    ShardSelector.main(Array(
      s"$tmpDir/test",
      "--budget", "5"
    ))

    // then
    compareFilesBetweenDirectories(Seq(
      "test$[5].selection",
      "test$[5].selection.shard-count",
      "test$[5].selection.shard-count.avg",
      "test$[5].selected.docs",
      "test$[5].selected.scores"), resourcesPath, tmpDir.toString)
  }

  test("main: thresholds") {
    // given
    val tmpDir = createTemporaryCopyOfResources(regex = ".*sizes|.*results.*|.*scores|.*properties|.*queries|.*payoff|.*cost")

    // when
    ShardSelector.main(Array(
      s"$tmpDir/test",
      "--threshold", "5"
    ))

    // then

  }

}
