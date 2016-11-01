package edu.nyu.tandon.search.selective

import java.nio.file.{Files, Paths}

import edu.nyu.tandon.test.BaseFunSuite
import edu.nyu.tandon.utils.WriteLineIterator._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class Time2CostTest extends BaseFunSuite {

  trait Copy {
    val tmpDir = createTemporaryCopyOfResources(regex = "test#.\\.time|.*avg|.*sizes|.*properties")
    val toCompare = Seq(
      "features/test#0.cost",
      "features/test#1.cost",
      "features/test#2.cost"
    )
  }
  trait FromAvg extends Copy {
    val expectedDir = createTemporaryDir()
    val expected = Seq(
      Seq(1, 1),
      Seq(1, 1),
      Seq(1, 1)
    )
    Files.createDirectory(Paths.get(expectedDir.toString, "features"))
    for ((file, seq) <- toCompare.zip(expected)) seq.iterator.map(_.toDouble).write(s"$expectedDir/$file")
  }

  trait UnitCost5 extends Copy {
    val expectedDir = createTemporaryDir()
    val expected = Seq(
      Seq(1.2, 1.2),
      Seq(1.2, 1.2),
      Seq(1.2, 1.2)
    )
    Files.createDirectory(Paths.get(expectedDir.toString, "features"))
    for ((file, seq) <- toCompare.zip(expected)) seq.iterator.write(s"$expectedDir/$file")
  }

  test("time2cost") {
    new FromAvg {
      Time2Cost.main(Array(s"$tmpDir/test"))
      compareFilesBetweenDirectories(toCompare, expectedDir.toString, tmpDir.toString)
    }
  }

  test("time2cost --unit-cost 5") {
    new UnitCost5 {
      Time2Cost.main(Array("--unit-cost", "5", s"$tmpDir/test"))
      compareFilesBetweenDirectories(toCompare, expectedDir.toString, tmpDir.toString)
    }
  }

}
