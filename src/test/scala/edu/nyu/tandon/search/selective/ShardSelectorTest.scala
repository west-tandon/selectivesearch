package edu.nyu.tandon.search.selective

import java.io.ByteArrayOutputStream

import edu.nyu.tandon.search.selective.data.{Bin, QueryShardExperiment}
import org.scalatest.FunSuite

/**
  * @author michal.siedlaczek@nyu.edu
  */
class ShardSelectorTest extends FunSuite {

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
        List(2, 0, 0),
        List(1, 2, 2)
      ))
    }
  }

  test("write selector") {
    new Selector {
      // given
      val os = new ByteArrayOutputStream()

      // when
      selector.write(os)

      // then
      assert(os.toString === new StringBuilder()
        .append("0 3 2\n")
        .append("2 0 0\n")
        .append("1 2 2\n")
        .toString())
    }
  }

}
