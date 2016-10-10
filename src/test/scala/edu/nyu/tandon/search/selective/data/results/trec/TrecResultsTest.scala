package edu.nyu.tandon.search.selective.data.results.trec

import java.nio.file.Files

import edu.nyu.tandon.test.BaseFunSuite
import org.scalatest.Matchers._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class TrecResultsTest extends BaseFunSuite {

  trait Trec {
    val expected = Seq(
      "701\tQ0\tGX000-21-7532716\t0\t82.0\tnull",
      "701\tQ0\tGX000-11-1485521\t1\t81.0\tnull",
      "701\tQ0\tGX000-22-7107523\t2\t72.0\tnull",
      "701\tQ0\tGX000-12-1160327\t3\t71.0\tnull",
      "701\tQ0\tGX000-23-7881059\t4\t62.0\tnull",
      "701\tQ0\tGX000-13-1378099\t5\t61.0\tnull",
      "701\tQ0\tGX001-38-5707695\t6\t52.0\tnull",
      "701\tQ0\tGX001-27-9361391\t7\t51.0\tnull",
      "701\tQ0\tGX001-39-6690164\t8\t42.0\tnull",
      "701\tQ0\tGX001-28-9761580\t9\t41.0\tnull",
      "701\tQ0\tGX001-40-9293997\t10\t32.0\tnull",
      "701\tQ0\tGX001-29-11890448\t11\t31.0\tnull",
      "701\tQ0\tGX002-41-7091392\t12\t21.0\tnull",
      "701\tQ0\tGX002-42-8658209\t13\t11.0\tnull",
      "702\tQ0\tGX000-00-15432219\t0\t80.0\tnull",
      "702\tQ0\tGX000-01-14704660\t1\t70.0\tnull",
      "702\tQ0\tGX000-02-15104424\t2\t60.0\tnull",
      "702\tQ0\tGX001-16-10305589\t3\t50.0\tnull",
      "702\tQ0\tGX001-17-11829827\t4\t40.0\tnull",
      "702\tQ0\tGX001-18-13333870\t5\t30.0\tnull")
  }

  test("fromTrecFile") {
    new Trec {
      TrecResults.fromTrecFile(s"$resourcesPath/test.selected.trec").map(_.toString).toSeq should contain theSameElementsInOrderAs expected
    }
  }

  test("fromSelected") {
    new Trec {
      TrecResults.fromSelected(s"$resourcesPath/test").map(_.toString).toSeq should contain theSameElementsInOrderAs expected
    }
  }

  test("save") {
    // given
    val tmpDir = Files.createTempDirectory(null)
    val name = "test.selected.trec"

    // when
    val t = TrecResults.fromTrecFile(s"$resourcesPath/$name")
    t.saveAs(s"$tmpDir/$name")

    // then
    compareFilesBetweenDirectories(Seq(name), tmpDir.toString, resourcesPath)
  }

}
