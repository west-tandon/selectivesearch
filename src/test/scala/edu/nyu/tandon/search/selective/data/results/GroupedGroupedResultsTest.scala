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
        Seq(11, 12, 13, 114, 115, 116, 217, 218, 21, 22, 23, 124, 125, 126),
        Seq(1, 2, 3, 104, 105, 106),
        Seq(1, 2, 3, 11, 12, 13, 114, 115, 116, 21, 22, 23, 124, 125, 126)
      )) actual.map(_.documentId) should contain theSameElementsInOrderAs expected
    }
  }

}
