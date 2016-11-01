package edu.nyu.tandon.search.selective.data.results

import edu.nyu.tandon.test.BaseFunSuite
import org.scalatest.Matchers._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class ResultLineTest extends BaseFunSuite {

  trait ResultsWithScores {
    val results = ResultLine.get(Seq(1, 2, 3, 4, 5), Seq(10, 20, 30, 40, 50), Seq(5, 4, 3, 2, 1))
  }

  test("fromString") {
    new ResultsWithScores {
      results.toSeq should contain theSameElementsInOrderAs Seq(
        Result(1, 10, 5.0),
        Result(2, 20, 4.0),
        Result(3, 30, 3.0),
        Result(4, 40, 2.0),
        Result(5, 50, 1.0)
      )
    }
  }

}
