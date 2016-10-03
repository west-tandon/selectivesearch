package edu.nyu.tandon.search.selective.data.results

import edu.nyu.tandon.test.BaseFunSuite
import org.scalatest.Matchers._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class GroupedGroupedResultsTest extends BaseFunSuite {

  trait Selector {
    val selector = Seq(
      Seq(0, 3, 2),
      Seq(2, 0, 0),
      Seq(1, 2, 2)
    )
  }

  test("select") {
    new Selector {
      for ((actual, expected) <- resultsByShardsAndBinsFromBasename(s"$resourcesPath/test").select(selector) zip Seq(
        Seq(21, 11, 22, 12, 23, 13, 124, 114, 125, 115, 126, 116, 217, 218),
        Seq(1, 2, 3, 104, 105, 106),
        Seq(21, 11, 1, 22, 12, 2, 23, 13, 3, 124, 114, 125, 115, 126, 116)
      )) actual.map(_.documentId) should contain theSameElementsInOrderAs expected
    }
  }

}
