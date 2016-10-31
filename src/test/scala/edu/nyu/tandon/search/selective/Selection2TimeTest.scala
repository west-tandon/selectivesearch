package edu.nyu.tandon.search.selective

import edu.nyu.tandon.test.BaseFunSuite

/**
  * @author michal.siedlaczek@nyu.edu
  */
class Selection2TimeTest extends BaseFunSuite {

  trait Copy {
    val tmpDir = createTemporaryCopyOfResources(regex = "test#.\\.time|.*sizes|.*properties|.*selection")
    val toCompare = Seq(
      s"test$BudgetIndicator[5.0].selection.time",
      s"test$BudgetIndicator[5.0].selection.time.avg"
    )
  }

  test("selection2time") {
    new Copy {
      Selection2Time.main(Array(s"$tmpDir/test$BudgetIndicator[5.0]"))
      compareFilesBetweenDirectories(toCompare, resourcesPath, tmpDir.toString)
    }
  }

}
