package edu.nyu.tandon.search.selective.data.payoff

import edu.nyu.tandon.search.selective.learn.LearnPayoffs
import edu.nyu.tandon.test.BaseFunSuite
import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.sql.SparkSession
import org.scalatest.Matchers._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class PayoffsTest extends BaseFunSuite {

  trait ExpectedPayoffs {
    val expected = Seq(
      Seq(
        Seq(1, 0, 0),
        Seq(3, 2, 1),
        Seq(2, 1, 0)
      ),
      Seq(
        Seq(3, 3, 0),
        Seq(0, 0, 0),
        Seq(0, 0, 0)
      )
    )
  }

  test("fromPayoffs") {
    new ExpectedPayoffs {
      val actual = Payoffs.fromPayoffs(s"$resourcesPath/test")
      for ((sa, se) <- actual.zipAll(expected, Seq(), Seq()))
        for ((a, e) <- sa.zipAll(se, Seq(), Seq()))
          a should contain theSameElementsAs e
    }
  }

  test("fromPayoffs/store") {
    val tempDir = createTemporaryCopyOfResources(".*properties")
    Payoffs.fromPayoffs(s"$resourcesPath/test").store(s"${tempDir.toString}/test")
    compareFilesBetweenDirectories(for (s <- 0 until 3; b <- 0 until 3) yield s"test#$s#$b.payoff",
      tempDir.toString, resourcesPath)
  }

  test("fromResults") {
    new ExpectedPayoffs {
      val actual = Payoffs.fromResults(s"$resourcesPath/test")
      for ((sa, se) <- actual.zipAll(expected, Seq(), Seq()))
        for ((a, e) <- sa.zipAll(se, Seq(), Seq()))
          a should contain theSameElementsAs e
    }
  }

  test("fromRegressionModel") {
    new ExpectedPayoffs {
      SparkSession.builder()
        .master("local[*]")
        .appName(LearnPayoffs.CommandName)
        .getOrCreate()
      val actual = Payoffs.fromRegressionModel(s"$resourcesPath/test",
        RandomForestRegressionModel.load(s"$resourcesPath/test.model"))
      actual.toSeq.length shouldBe 2
      for (sa <- actual) {
        sa.length shouldBe 3
        for (a <- actual)
          a.length shouldBe 3
      }
    }
  }

}
