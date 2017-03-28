package edu.nyu.tandon.search.selective

import java.nio.file.{Files, Paths}

import edu.nyu.tandon.search.selective.BucketizeResults._
import edu.nyu.tandon.search.selective.Path._
import edu.nyu.tandon.test.BaseFunSuite
import edu.nyu.tandon.utils.Lines
import org.scalatest.Matchers._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class BucketizeResultsTest extends BaseFunSuite {

  trait FilesWithScores {
    val outputFileNames = Seq(
      Seq(
        s"test#0#0$ResultsSuffix$LocalSuffix",
        s"test#0#1$ResultsSuffix$LocalSuffix",
        s"test#0#2$ResultsSuffix$LocalSuffix",
        s"test#0#0$ResultsSuffix$GlobalSuffix",
        s"test#0#1$ResultsSuffix$GlobalSuffix",
        s"test#0#2$ResultsSuffix$GlobalSuffix",
        s"test#0#0$ResultsSuffix$ScoresSuffix",
        s"test#0#1$ResultsSuffix$ScoresSuffix",
        s"test#0#2$ResultsSuffix$ScoresSuffix"
      ),
      Seq(
        s"test#1#0$ResultsSuffix$LocalSuffix",
        s"test#1#1$ResultsSuffix$LocalSuffix",
        s"test#1#2$ResultsSuffix$LocalSuffix",
        s"test#1#0$ResultsSuffix$GlobalSuffix",
        s"test#1#1$ResultsSuffix$GlobalSuffix",
        s"test#1#2$ResultsSuffix$GlobalSuffix",
        s"test#1#0$ResultsSuffix$ScoresSuffix",
        s"test#1#1$ResultsSuffix$ScoresSuffix",
        s"test#1#2$ResultsSuffix$ScoresSuffix"
      ),
      Seq(
        s"test#2#0$ResultsSuffix$LocalSuffix",
        s"test#2#1$ResultsSuffix$LocalSuffix",
        s"test#2#2$ResultsSuffix$LocalSuffix",
        s"test#2#0$ResultsSuffix$GlobalSuffix",
        s"test#2#1$ResultsSuffix$GlobalSuffix",
        s"test#2#2$ResultsSuffix$GlobalSuffix",
        s"test#2#0$ResultsSuffix$ScoresSuffix",
        s"test#2#1$ResultsSuffix$ScoresSuffix",
        s"test#2#2$ResultsSuffix$ScoresSuffix"
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

  test("main: shard with costs") {
    new FilesWithScores {
      // given
      val tmpDir = createTemporaryCopyOfResources(regex = "test#.\\.cost|test\\.sizes|test#.\\.results.*|.*properties|.*queries")

      // when
      BucketizeResults.main(Array("--docrank=false", s"$tmpDir/test#0"))

      // then
      compareFilesBetweenDirectories(outputFileNames.head, getClass.getResource("/").getPath, tmpDir.toString)
      for (s <- outputFileNames.drop(1))
        for (f <- s) Files.exists(Paths.get(s"${tmpDir.toString}/$f")) shouldBe false
    }
  }

  test("main: shard without costs") {
    new FilesWithScores {
      // given
      val tmpDir = createTemporaryCopyOfResources(regex = "test\\.sizes|test#.\\.results.*|.*properties|.*queries")

      // when
      BucketizeResults.main(Array("--docrank=false", "--cost=false", s"$tmpDir/test#0"))

      // then
      compareFilesBetweenDirectories(outputFileNames.head, getClass.getResource("/").getPath, tmpDir.toString)
      for (s <- outputFileNames.drop(1))
        for (f <- s) Files.exists(Paths.get(s"${tmpDir.toString}/$f")) shouldBe false
    }
  }

  test("main: all with costs") {
    new FilesWithScores {
      // given
      val tmpDir = createTemporaryCopyOfResources(regex = "test#.\\.cost|test\\.sizes|test#.\\.results.*|test#.\\.scores|.*properties|.*queries|test#.\\.docrank")

      // when
      BucketizeResults.main(Array(s"$tmpDir/test"))

      // then
      compareFilesBetweenDirectories(outputFileNames.flatten, getClass.getResource("/").getPath, tmpDir.toString)
      Lines.fromFile(s"$tmpDir/test#0#0.bucketrank").of[Int](_.toInt).toList should contain theSameElementsInOrderAs List(49, 49)
      Lines.fromFile(s"$tmpDir/test#0#1.bucketrank").of[Int](_.toInt).toList should contain theSameElementsInOrderAs List(149, 149)
      Lines.fromFile(s"$tmpDir/test#0#2.bucketrank").of[Int](_.toInt).toList should contain theSameElementsInOrderAs List(249, 249)
    }
  }

  test("main: all without costs") {
    new FilesWithScores {
      // given
      val tmpDir = createTemporaryCopyOfResources(regex = "test\\.sizes|test#.\\.results.*|test#.\\.scores|.*properties|.*queries")

      // when
      BucketizeResults.main(Array("--docrank=false", "--cost=false", s"$tmpDir/test"))

      // then
      compareFilesBetweenDirectories(outputFileNames.flatten, getClass.getResource("/").getPath, tmpDir.toString)
    }
  }

}
