package edu.nyu.tandon.search.selective.data.results

import edu.nyu.tandon.test.BaseFunSuite
import org.scalatest.Matchers._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class ResultLineTest extends BaseFunSuite {

  trait ResultsWithScores {
    val results = ResultLine.fromString("1 2 3 4 5", "10 20 30 40 50", "5 4 3 2 1")
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

  test("partition: with scores") {
//    new ResultsWithScores {
//      val grouped = results.groupByBuckets(bucketSize = 2, bucketCount = 3)
//      for (group <- grouped) group.query shouldBe results.query
//      grouped.head.localDocumentIds should contain theSameElementsInOrderAs Seq(1)
//      grouped.head.getScores should contain theSameElementsInOrderAs Seq(5.0)
//      grouped.drop(1).head.localDocumentIds should contain theSameElementsInOrderAs Seq(2, 3)
//      grouped.drop(1).head.getScores should contain theSameElementsInOrderAs Seq(4.0, 3.0)
//      grouped.drop(2).head.localDocumentIds should contain theSameElementsInOrderAs Seq(4, 5)
//      grouped.drop(2).head.getScores should contain theSameElementsInOrderAs Seq(2.0, 1.0)
//    }
  }

}
