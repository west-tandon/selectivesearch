package edu.nyu.tandon.search.selective

import edu.nyu.tandon.test.BaseFunSuite
import org.scalatest.Matchers._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class PrecisionTest extends BaseFunSuite {

  test("calcOverlaps") {
    // given
    val ref = Seq(
      Seq(1000, 11000, 12000, 13000, 114000, 115000, 217000, 21000, 22000, 124000),
      Seq(1000, 2000, 3000, 104000, 105000, 106000)
    ).iterator.map(_.map(_.toLong))
    val in = Seq(
      Seq(21000, 11000, 22000, 12000, 23000, 13000, 124000, 114000, 125000, 115000,
        126000, 116000, 217000, 218000),
      Seq(1000, 2000, 3000, 104000, 105000, 106000)
    ).iterator.map(_.map(_.toLong))

    // when
    val precisions = Precision.precision(in, ref)(Seq(10, 30))

    // then
    precisions.next() should contain theSameElementsAs Seq(0.8, 0.3)
    precisions.next() should contain theSameElementsAs Seq(0.6, 0.2)
  }

}
