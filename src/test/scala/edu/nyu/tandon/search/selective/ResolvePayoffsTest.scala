package edu.nyu.tandon.search.selective

import edu.nyu.tandon.test.BaseFunSuite

/**
  * @author michal.siedlaczek@nyu.edu
  */
class ResolvePayoffsTest extends BaseFunSuite {

  test("main") {
    // given
    val tmpDir = createTemporaryCopyOfResources(regex = ".*sizes|.*results.*|.*properties")

    // when
    ResolvePayoffs.main(Array("--basename", s"$tmpDir/test"))

    // then
    compareFilesBetweenDirectories(for (s <- 0 until 3; b <- 0 until 3) yield s"test#$s#$b.payoff",
      getClass.getResource("/").getPath, tmpDir.toString)
  }

}
