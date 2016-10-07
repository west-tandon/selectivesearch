package edu.nyu.tandon.search.selective

import java.nio.file.{Files, Paths}

import edu.nyu.tandon.search.selective.BucketizeResults._
import edu.nyu.tandon.test.BaseFunSuite
import org.scalatest.Matchers._

import scala.io.Source

/**
  * @author michal.siedlaczek@nyu.edu
  */
class BucketizeResultsTest extends BaseFunSuite {

  trait FilesWithScores {
    val outputFileNames = Seq(
      Seq(
        s"test#0#0$ResultsSuffix",
        s"test#0#1$ResultsSuffix",
        s"test#0#2$ResultsSuffix",
        s"test#0#0$ScoresSuffix",
        s"test#0#1$ScoresSuffix",
        s"test#0#2$ScoresSuffix"
      ),
      Seq(
        s"test#1#0$ResultsSuffix",
        s"test#1#1$ResultsSuffix",
        s"test#1#2$ResultsSuffix",
        s"test#1#0$ScoresSuffix",
        s"test#1#1$ScoresSuffix",
        s"test#1#2$ScoresSuffix"
      ),
      Seq(
        s"test#2#0$ResultsSuffix",
        s"test#2#1$ResultsSuffix",
        s"test#2#2$ResultsSuffix",
        s"test#2#0$ScoresSuffix",
        s"test#2#1$ScoresSuffix",
        s"test#2#2$ScoresSuffix"
      )
    )
  }

  trait FilesWithoutScores {
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

  test("main: shard with scores") {
    new FilesWithScores {
      // given
      val tmpDir = createTemporaryCopyOfResources(regex = "test#.\\.results|test#.\\.scores|.*properties|.*queries")

      // when
      BucketizeResults.main(Array("--basename", s"$tmpDir/test#0"))

      // then
      compareFilesBetweenDirectories(outputFileNames.head, getClass.getResource("/").getPath, tmpDir.toString)
      for (s <- outputFileNames.drop(1))
        for (f <- s) Files.exists(Paths.get(s"${tmpDir.toString}/$f")) shouldBe false
    }
  }

  test("main: shard without scores") {
    new FilesWithoutScores {
      // given
      val tmpDir = createTemporaryCopyOfResources(regex = "test#.\\.results|.*properties|.*queries")

      // when
      BucketizeResults.main(Array("--basename", s"$tmpDir/test#0"))

      // then
      compareFilesBetweenDirectories(outputFileNames.head, getClass.getResource("/").getPath, tmpDir.toString)
    }
  }

  test("main: all with scores") {
    new FilesWithScores {
      // given
      val tmpDir = createTemporaryCopyOfResources(regex = "test#.\\.results|test#.\\.scores|.*properties|.*queries")

      // when
      BucketizeResults.main(Array("--basename", s"$tmpDir/test"))

      // then
      compareFilesBetweenDirectories(outputFileNames.flatten, getClass.getResource("/").getPath, tmpDir.toString)
    }
  }

  test("main: all without scores") {
    new FilesWithoutScores {
      // given
      val tmpDir = createTemporaryCopyOfResources(regex = "test#.\\.results|.*properties|.*queries")

      // when
      BucketizeResults.main(Array("--basename", s"$tmpDir/test"))

      // then
      compareFilesBetweenDirectories(outputFileNames.flatten, getClass.getResource("/").getPath, tmpDir.toString)
    }
  }

}
