package edu.nyu.tandon.search.selective

import edu.nyu.tandon.test.BaseFunSuite
import edu.nyu.tandon.utils.Lines
import edu.nyu.tandon.utils.Lines._
import org.scalatest.Matchers._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class PenalizeTest extends BaseFunSuite {

  test("penalize -p 5") {
    // given
    val tmpDir = createTemporaryCopyOfResources(regex = "test#.#.\\.cost|.*sizes|.*properties")

    // when
    Penalize.main(Array("-p", "5", s"$tmpDir/test"))

    // then
    for (shard <- 0 until 3)
      Lines.fromFile(s"$tmpDir/test#$shard#0.cost").of[Double].toSeq should contain theSameElementsInOrderAs Seq(6.0, 6)
  }

}
