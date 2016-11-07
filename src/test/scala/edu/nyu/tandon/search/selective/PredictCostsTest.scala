package edu.nyu.tandon.search.selective

import edu.nyu.tandon.search.selective.learn.PredictCosts
import edu.nyu.tandon.test.BaseFunSuite

/**
  * @author michal.siedlaczek@nyu.edu
  */
class PredictCostsTest extends BaseFunSuite {

  test("main: run and not fail") {
    // given
    val tmpDir = createTemporaryCopyOfResources(regex = ".*")

    // when
    PredictCosts.main(Array(
      "--model", s"$tmpDir/test.cost-model",
      s"$tmpDir/test"
    ))
  }

}
