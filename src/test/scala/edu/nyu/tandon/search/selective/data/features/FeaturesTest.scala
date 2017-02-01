package edu.nyu.tandon.search.selective.data.features

import edu.nyu.tandon.search.selective.data.Properties
import edu.nyu.tandon.search.selective.data.results.Result
import edu.nyu.tandon.test.BaseFunSuite
import org.scalatest.Matchers._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class FeaturesTest extends BaseFunSuite {

  trait F {
    val f = Properties.get(s"$resourcesPath/test").features
  }

  test("shardCount") {
    new F {
      f.shardCount shouldBe 3
    }
  }

  test("shardSizes") {
    new F {
      f.shardSizes should contain theSameElementsInOrderAs Seq(300, 300, 300)
    }
  }

  test("reddeScores") {
    new F {
      val scores = f.reddeScores.toIndexedSeq
      scores.length shouldBe 2
      scores(0) should contain theSameElementsInOrderAs Seq(0.1, 0.1, 0.1)
      scores(1) should contain theSameElementsInOrderAs Seq(0.5, 0.5, 0.5)
    }
  }

  test("shrkcScores") {
    new F {
      val scores = f.shrkcScores.toIndexedSeq
      scores.length shouldBe 2
      scores(0) should contain theSameElementsInOrderAs Seq(0.001, 0.001, 0.001)
      scores(1) should contain theSameElementsInOrderAs Seq(0.005, 0.005, 0.005)
    }
  }

  test("queries") {
    new F {
      f.queries.toSeq should contain theSameElementsInOrderAs Seq("query one", "query two")
    }
  }

  test("queryLengths") {
    new F {
      f.queryLengths.toSeq should contain theSameElementsInOrderAs Seq(2, 2)
    }
  }

  test("trecIds") {
    new F {
      f.trecIds.toSeq should contain theSameElementsInOrderAs Seq(701, 702)
    }
  }

  test("documentTitles: size") {
    new F {
      f.documentTitles.length shouldBe 300000
    }
  }

  test("documentTitles: head") {
    new F {
      f.documentTitles.next() shouldBe "GX000-00-0000000"
    }
  }

  test("baseResults") {
    new F {
      new F {
        val results = f.baseResults.toIndexedSeq
        results.length shouldBe 2
        results(0) should contain theSameElementsInOrderAs Seq(1000, 11000, 12000, 13000, 114000, 115000, 217000, 21000, 22000, 124000)
        results(1) should contain theSameElementsInOrderAs Seq(1000, 2000, 3000, 104000, 105000, 106000)
      }
    }
  }

  test("shardResults") {
    new F {
      val results = f.shardResults.toIndexedSeq
      results.length shouldBe 2
      results(0).head should contain theSameElementsInOrderAs Seq(
        Result(1, 1000, 80.0),
        Result(2, 2000, 70.0),
        Result(3, 3000, 60.0),
        Result(104, 104000, 50.0),
        Result(105, 105000, 40.0),
        Result(106, 106000, 30.0),
        Result(207, 207000, 20.0),
        Result(208, 208000, 10.0),
        Result(299, 9999999, 0.99)
      )
    }
  }

  test("costs") {
    new F {
      val costs = f.costs.toIndexedSeq
      costs.length shouldBe 2
      costs(0) should contain theSameElementsInOrderAs Seq(3, 3, 3)
      costs(1) should contain theSameElementsInOrderAs Seq(3, 3, 3)
    }
  }

}
