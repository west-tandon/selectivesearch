package edu.nyu.tandon.search.selective.data.payoff

import edu.nyu.tandon.test.BaseFunSuite
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
      for ((sa, se) <- actual.zip(expected))
        for ((a, e) <- sa.zip(se))
          a should contain theSameElementsAs e
    }
  }

  test("fromPayoofs/store") {
    val tempDir = createTemporaryCopyOfResources(".*properties")
    Payoffs.fromPayoffs(s"$resourcesPath/test").store(s"${tempDir.toString}/test")
    compareFilesBetweenDirectories(Seq(), tempDir.toString, resourcesPath)
  }

  test("fromResults") {
    new ExpectedPayoffs {
      val actual = Payoffs.fromResults(s"$resourcesPath/test")
      for ((sa, se) <- actual.zip(expected))
        for ((a, e) <- sa.zip(se))
          a should contain theSameElementsAs e
    }
  }

}
