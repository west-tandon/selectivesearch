package edu.nyu.tandon.search.selective

import edu.nyu.tandon.search.selective.data.{Bin, QueryShardExperiment}
import edu.nyu.tandon.test.BaseFunSuite

/**
  * @author michal.siedlaczek@nyu.edu
  */
class ShardSelectorTest extends BaseFunSuite {

  trait Selector {
    val queryExperiment = QueryShardExperiment.fromBasename(getClass.getResource("/").getPath + "test")
    val selector = new ShardSelector(queryExperiment, 5)
  }

  trait Bins {
    val bins = List(
      Bin(0, 9, 9),
      Bin(1, 8, 8),
      Bin(0, 5, 5),
      Bin(1, 2, 2),
      Bin(0, 1, 1)
    )
  }

  test("binsWithinBudget") {
    new Bins {
      // given
      val budget = 17.5

      // when
      val bwb = ShardSelector.binsWithinBudget(bins, budget)

      // then
      assert(bwb == Seq(Bin(0, 9, 9), Bin(1, 8, 8)))
    }
  }

  test("binsWithinBudget: budget even with the sum of costs") {
    new Bins {
      // given
      val budget = 17

      // when
      val bwb = ShardSelector.binsWithinBudget(bins, budget)

      // then
      assert(bwb == Seq(Bin(0, 9, 9), Bin(1, 8, 8)))
    }
  }

  /*
   * Still should return the first bin.
   */
  test("binsWithinBudget: budget lower than the first cost") {
    new Bins {
      // given
      val budget = 5

      // when
      val bwb = ShardSelector.binsWithinBudget(bins, budget)

      // then
      assert(bwb == Seq(Bin(0, 9, 9)))
    }
  }

  test("selector") {
    new Selector {
      // when
      val l = selector.toList

      // then
      assert(l === List(
        List(0, 3, 2),
        List(2, 0, 0)
      ))
    }
  }

  test("main: with scores") {
    // given
    val tmpDir = createTemporaryCopyOfResources(regex = ".*results|.*scores|.*properties|.*queries|.*payoff|.*cost")

    // when
    ShardSelector.main(Array(
      "--basename", s"$tmpDir/test",
      "--budget", "5"
    ))

    // then
    compareFilesBetweenDirectories(Seq("test.selection", "test.selected.results", "test.selected.scores"), resourcesPath, tmpDir.toString)
  }

  test("main: without scores") {
    // given
    val tmpDir = createTemporaryCopyOfResources(regex = ".*results|.*properties|.*queries|.*payoff|.*cost")

    // when
    ShardSelector.main(Array(
      "--basename", s"$tmpDir/test",
      "--budget", "5"
    ))

    // then
    compareFilesBetweenDirectories(Seq("test.selection"), resourcesPath, tmpDir.toString)
  }

}
