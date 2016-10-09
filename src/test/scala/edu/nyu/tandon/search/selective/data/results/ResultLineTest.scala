package edu.nyu.tandon.search.selective.data.results

import edu.nyu.tandon.test.BaseFunSuite
import org.scalatest.Matchers._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class ResultLineTest extends BaseFunSuite {

  trait ResultsWithScores {
    val results = ResultLine.fromString("q1", "1 2 3 4 5", "10 20 30 40 50", "5 4 3 2 1")
  }

  trait ResultsWithoutScores {
    val results = ResultLine.fromString("q2", "1 2 3 4 5", "10 20 30 40 50")
  }

  trait EmptyResults {
    val results = ResultLine.fromString("q3", "", "")
  }

  test("fromString: empty scores") {
    new EmptyResults {
      results.query shouldBe "q3"
    }
  }

  test("fromString: with scores") {
    new ResultsWithScores {
      results.query shouldBe "q1"
      results.localDocumentIds should contain theSameElementsInOrderAs Seq(1, 2, 3, 4, 5)
      results.globalDocumentIds should contain theSameElementsInOrderAs Seq(10, 20, 30, 40, 50)
      results.getScores should contain theSameElementsInOrderAs Seq(5.0, 4.0, 3.0, 2.0, 1.0)
    }
  }

  test("fromString: without scores") {
    new ResultsWithoutScores {
      results.query shouldBe "q2"
      results.localDocumentIds should contain theSameElementsInOrderAs Seq(1, 2, 3, 4, 5)
      results.globalDocumentIds should contain theSameElementsInOrderAs Seq(10, 20, 30, 40, 50)
      results.hasScores shouldBe false
    }
  }

  test("toStringTuple: with scores") {
    new ResultsWithScores {
      results.toStringTuple shouldBe ("q1", "1 2 3 4 5", "10 20 30 40 50", Some("5.0 4.0 3.0 2.0 1.0"))
    }
  }

  test("toStringTuple: without scores") {
    new ResultsWithoutScores {
      results.toStringTuple shouldBe ("q2", "1 2 3 4 5", "10 20 30 40 50", None)
    }
  }

  test("iterator: with scores") {
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

  test("iterator: without scores") {
    new ResultsWithoutScores {
      results.toSeq should contain theSameElementsInOrderAs Seq(
        Result(1, 10, None),
        Result(2, 20, None),
        Result(3, 30, None),
        Result(4, 40, None),
        Result(5, 50, None)
      )
    }
  }

  test("partition: with scores") {
    new ResultsWithScores {
      val grouped = results.groupByBuckets(bucketSize = 2, bucketCount = 3)
      for (group <- grouped) group.query shouldBe results.query
      grouped.head.localDocumentIds should contain theSameElementsInOrderAs Seq(1)
      grouped.head.getScores should contain theSameElementsInOrderAs Seq(5.0)
      grouped.drop(1).head.localDocumentIds should contain theSameElementsInOrderAs Seq(2, 3)
      grouped.drop(1).head.getScores should contain theSameElementsInOrderAs Seq(4.0, 3.0)
      grouped.drop(2).head.localDocumentIds should contain theSameElementsInOrderAs Seq(4, 5)
      grouped.drop(2).head.getScores should contain theSameElementsInOrderAs Seq(2.0, 1.0)
    }
  }

  test("partition: without scores") {
    new ResultsWithoutScores {
      val grouped = results.groupByBuckets(bucketSize = 2, bucketCount = 3)
      for (group <- grouped) group.query shouldBe results.query
      grouped.head.localDocumentIds should contain theSameElementsInOrderAs Seq(1)
      grouped.head.hasScores shouldBe false
      grouped.drop(1).head.localDocumentIds should contain theSameElementsInOrderAs Seq(2, 3)
      grouped.drop(1).head.hasScores shouldBe false
      grouped.drop(2).head.localDocumentIds should contain theSameElementsInOrderAs Seq(4, 5)
      grouped.drop(2).head.hasScores shouldBe false
    }
  }

}
