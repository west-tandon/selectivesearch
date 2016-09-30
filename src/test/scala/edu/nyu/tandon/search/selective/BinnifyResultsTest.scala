package edu.nyu.tandon.search.selective

import java.nio.file.{Files, Paths}

import edu.nyu.tandon.search.selective.BinnifyResults._
import edu.nyu.tandon.test.BaseFunSuite
import org.scalatest.Matchers._

import scala.io.Source

/**
  * @author michal.siedlaczek@nyu.edu
  */
class BinnifyResultsTest extends BaseFunSuite {

  trait Files {
    val outputFileNames = Seq(
      Seq(
        s"test#0#0$ResultsSuffix",
        s"test#0#1$ResultsSuffix",
        s"test#0#2$ResultsSuffix"
      ),
      Seq(
        s"test#1#0$ResultsSuffix",
        s"test#1#1$ResultsSuffix",
        s"test#1#2$ResultsSuffix"
      ),
      Seq(
        s"test#2#0$ResultsSuffix",
        s"test#2#1$ResultsSuffix",
        s"test#2#2$ResultsSuffix"
      )
    )
  }

  test("parseBasename: no #") {
    parseBasename("test") shouldBe ("test", None)
  }

  test("parseBasename: one #") {
    parseBasename("test#2") shouldBe ("test", Some(2))
  }

  test("parseBasename: two #'s") {
    intercept [IllegalStateException] {
      parseBasename("test#2#4")
    }
  }

  test("groupByBins") {
    groupByBins(Array(1, 2, 3, 99, 100, 101, 104, 105, 106, 199, 200, 201, 207, 208), 100, 3) should contain theSameElementsInOrderAs List(
      Array(1, 2, 3, 99),
      Array(100, 101, 104, 105, 106, 199),
      Array(200, 201, 207, 208)
    )
  }

  test("binnifyShard") {
    // given
    val expected = Seq(
      Vector(
        Array(0, 1),
        Array(100, 101),
        Array(200, 201)
      )
    )

    // when
    val actual = binnifyShard(Source.fromString("0 1 100 101 200 201"), 3, 300).toSeq

    // then
    actual.length shouldBe expected.length
    actual.head should contain theSameElementsInOrderAs expected.head
  }

  test("binnifyShard: from file") {
    // given
    val expected = Seq(
      Vector(
        Array(1, 2, 3),
        Array(104, 105, 106),
        Array(207, 208)
      ),
      Vector(
        Array(1, 2, 3),
        Array(104, 105, 106),
        Array(207, 208)
      ),
      Vector(
        Array(1, 2, 3),
        Array(104, 105, 106),
        Array(207, 208)
      )
    )

    // when
    val actual = binnifyShard(getClass.getResource("/").getPath + "test", 0, 3, 300).toSeq

    // then
    actual.length shouldBe expected.length
    for ((a, e) <- (actual, expected).zipped) a should contain theSameElementsInOrderAs e
  }



  test("binnify") {
    // given
    val expected = Seq(
      Seq(
        Vector(
          Array(1, 2, 3),
          Array(104, 105, 106),
          Array(207, 208)
        ),
        Vector(
          Array(1, 2, 3),
          Array(104, 105, 106),
          Array(207, 208)
        ),
        Vector(
          Array(1, 2, 3),
          Array(104, 105, 106),
          Array(207, 208)
        )
      ),
      Seq(
        Vector(
          Array(11, 12, 13),
          Array(114, 115, 116),
          Array(217, 218)
        ),
        Vector(
          Array(11, 12, 13),
          Array(114, 115, 116),
          Array(217, 218)
        ),
        Vector(
          Array(11, 12, 13),
          Array(114, 115, 116),
          Array(217, 218)
        )
      ),
      Seq(
        Vector(
          Array(21, 22, 23),
          Array(124, 125, 126),
          Array(227, 228)
        ),
        Vector(
          Array(21, 22, 23),
          Array(124, 125, 126),
          Array(227, 228)
        ),
        Vector(
          Array(21, 22, 23),
          Array(124, 125, 126),
          Array(227, 228)
        )
      )
    )

    // when
    val actual = binnify(getClass.getResource("/").getPath + "test", 3, 3, 300)

    // then
    actual.length shouldBe expected.length
    for ((ai, ei) <- (actual, expected).zipped)
      for ((a, e) <- (ai.toSeq, ei).zipped)
        a should contain theSameElementsInOrderAs e
  }

  test("writeBinnifiedShard") {
    new Files {
      // given
      val tmpDir = Files.createTempDirectory(null)
      val binnified = Seq(
        Vector(
          Array(1, 2, 3),
          Array(104, 105, 106),
          Array(207, 208)
        ),
        Vector(
          Array(1, 2, 3),
          Array(104, 105, 106),
          Array(207, 208)
        ),
        Vector(
          Array(1, 2, 3),
          Array(104, 105, 106),
          Array(207, 208)
        )
      ).iterator map (_ map (_ map (_.toLong)))
      val files = outputFileNames.head

      // when
      writeBinnifiedShard(s"${tmpDir.toString}/test#0", 3, binnified)

      // then
      compareFilesBetweenDirectories(files, getClass.getResource("/").getPath, tmpDir.toString)
    }
  }

  test("main: shard") {
    new Files {
      // given
      val tmpDir = createTemporaryCopyOfResources(regex = "test#.\\.results|.*properties")

      // when
      BinnifyResults.main(Array("--basename", s"$tmpDir/test#0"))

      // then
      compareFilesBetweenDirectories(outputFileNames.head, getClass.getResource("/").getPath, tmpDir.toString)
      for (s <- outputFileNames.drop(1))
        for (f <- s) Files.exists(Paths.get(s"${tmpDir.toString}/$f")) shouldBe false
    }
  }

  test("main: all") {
    new Files {
      // given
      val tmpDir = createTemporaryCopyOfResources(regex = "test#.\\.results|.*properties")

      // when
      BinnifyResults.main(Array("--basename", s"$tmpDir/test"))

      // then
      compareFilesBetweenDirectories(outputFileNames.flatten, getClass.getResource("/").getPath, tmpDir.toString)
    }
  }

}
