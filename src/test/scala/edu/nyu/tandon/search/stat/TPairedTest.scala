package edu.nyu.tandon.search.stat

import java.io.File

import edu.nyu.tandon.test.BaseFunSuite
import edu.nyu.tandon.utils.Lines
import org.scalatest.Matchers._
import edu.nyu.tandon.utils.WriteLineIterator._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class TPairedTest extends BaseFunSuite {

  trait Data {
    val a = Array(1.0, 2.0, 5.0, 5.0, 2.0, 1.0)
    val b = Array(1.2, 2.2, 5.2, 5.2, 2.2, 1.2)
    val c = Array(0.8, 1.8, 4.8, 4.8, 1.8, 0.8)
  }

  trait Trec {
    val tempDir = createTemporaryDir()
    List(
      "m1\t1\t0.01",
      "m2\t1\t0.02",
      "m1\t2\t0.03",
      "m2\t2\t0.04",
      "m1\tall\t0.05",
      "m2\tall\t0.06"
    ).write(s"$tempDir/trec")
  }

  trait Overlap {
    val tempDir = createTemporaryDir()
    List(
      "0.1",
      "0.2"
    ).write(s"$tempDir/overlap")
  }

  test("pairedTTest(alpha=0.01)(a, b)") {
    new Data {
      TPaired.pairedTTest(alpha = 0.001)(a, b) shouldBe 0
    }
  }

  test("pairedTTest(alpha=0.4)(b, c)") {
    new Data {
      TPaired.pairedTTest(alpha = 0.4)(b, c) shouldBe 1
    }
  }

  test("pairedTTest(alpha=0.4)(c, b)") {
    new Data {
      TPaired.pairedTTest(alpha = 0.4)(c, b) shouldBe -1
    }
  }

  test("trec2values") {
    new Trec {
      TPaired.trec2values("m1")(Lines.fromFile(s"$tempDir/trec")).toList should contain theSameElementsInOrderAs List(
        0.01, 0.03
      )
      TPaired.trec2values("m2")(Lines.fromFile(s"$tempDir/trec")).toList should contain theSameElementsInOrderAs List(
        0.02, 0.04
      )
    }
  }

  test("readSample: trec") {
    new Trec {
      TPaired.readSample(new File(s"$tempDir/trec"), "m1").toList should contain theSameElementsInOrderAs List(
        0.01, 0.03
      )
      TPaired.readSample(new File(s"$tempDir/trec"), "m2").toList should contain theSameElementsInOrderAs List(
        0.02, 0.04
      )
    }
  }

  test("readSample: overlap") {
    new Overlap {
      TPaired.readSample(new File(s"$tempDir/overlap"), "overlap").toList should contain theSameElementsInOrderAs List(
        0.1, 0.2
      )
    }
  }

}
