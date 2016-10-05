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
      "701\tQ0\tGX000-00-0280942\t0\t82.0\tnull",
      "701\tQ0\tGX000-00-0114512\t1\t81.0\tnull",
      "701\tQ0\tGX000-00-0305363\t2\t72.0\tnull",
      "701\tQ0\tGX000-00-0146057\t3\t71.0\tnull",
      "701\tQ0\tGX000-00-0315746\t4\t62.0\tnull",
      "701\tQ0\tGX000-00-0177322\t5\t61.0\tnull",
      "701\tQ0\tGX000-00-1955437\t6\t52.0\tnull",
      "701\tQ0\tGX000-00-1781461\t7\t51.0\tnull",
      "701\tQ0\tGX000-00-2005888\t8\t42.0\tnull",
      "701\tQ0\tGX000-00-1796817\t9\t41.0\tnull",
      "701\tQ0\tGX000-00-2007490\t10\t32.0\tnull",
      "701\tQ0\tGX000-00-1818231\t11\t31.0\tnull",
      "701\tQ0\tGX000-00-3367656\t12\t21.0\tnull",
      "701\tQ0\tGX000-00-3377071\t13\t11.0\tnull",
      "702\tQ0\tGX000-00-0000000\t0\t80.0\tnull",
      "702\tQ0\tGX000-00-0000949\t1\t70.0\tnull",
      "702\tQ0\tGX000-00-0002361\t2\t60.0\tnull",
      "702\tQ0\tGX000-00-1603470\t3\t50.0\tnull",
      "702\tQ0\tGX000-00-1626741\t4\t40.0\tnull",
      "702\tQ0\tGX000-00-1644912\t5\t30.0\tnull")
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
