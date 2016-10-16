package edu.nyu.tandon.search.selective

import java.io.StringWriter

import edu.nyu.tandon.test.BaseFunSuite
import org.scalatest.Matchers._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class OverlapTest extends BaseFunSuite {

  test("writeAndCalcAvg") {
    // given
    val iterator = Seq(Seq(1.0, 4.0), Seq(2.0, 3.0), Seq(3.0, 2.0), Seq(4.0, 1.0)).iterator
    val writers = Seq(new StringWriter(), new StringWriter())

    // when
    val avg = Overlap.writeAndCalcAvgs(iterator, writers)

    // then
    avg shouldBe List(2.5, 2.5)
    writers.head.toString shouldBe "1.0\n2.0\n3.0\n4.0\n"
    writers.drop(1).head.toString shouldBe "4.0\n3.0\n2.0\n1.0\n"
  }

  test("calcOverlaps") {
    // given
    val ref = Seq(
      Seq(1000, 11000, 12000, 13000, 114000, 115000, 217000, 21000, 22000, 124000),
      Seq(1000, 2000, 3000, 104000, 105000, 106000)
    ).iterator.map(_.map(_.toLong))
    val in = Seq(
      Seq(21000, 11000, 22000, 12000, 23000, 13000, 124000, 114000, 125000, 115000, 126000, 116000, 217000, 218000),
      Seq(1000, 2000, 3000, 104000, 105000, 106000)
    ).iterator.map(_.map(_.toLong))

    // when
    val overlaps = Overlap.calcOverlaps(in, ref)

    // then
    overlaps.next() should contain theSameElementsAs Seq(0.8, 0.9, 0.9, 0.9, 0.9)
    overlaps.next() should contain theSameElementsAs Seq(1.0, 1.0, 1.0, 1.0, 1.0)
  }

  test("main") {
    // given
    val tmpDir = createTemporaryCopyOfResources(regex = ".*properties|.*selected.*|.*results.*")

    // when
    Overlap.main(Array(s"$tmpDir/test$BudgetIndicator[5.0]"))

    // then
    compareFilesBetweenDirectories(for (k <- Overlap.OverlapLevels) yield s"test$BudgetIndicator[5.0]@$k.overlaps",
      getClass.getResource("/").getPath, tmpDir.toString)
    compareFilesBetweenDirectories(for (k <- Overlap.OverlapLevels) yield s"test$BudgetIndicator[5.0]@$k.overlap",
      getClass.getResource("/").getPath, tmpDir.toString)
  }

}
