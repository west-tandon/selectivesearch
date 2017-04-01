package edu.nyu.tandon.utils

import edu.nyu.tandon.test.BaseFunSuite
import org.scalatest.Matchers._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class ReadLineIteratorTest extends BaseFunSuite {

  test("close") {

    // given
    val tmpDir = createTemporaryDir()
    val filename = s"$tmpDir/f"
    new WriteLineIterator(Seq("1", "2").iterator).write(filename)

    // when
    val readLineIterator = Lines.fromFile(filename)
    val seq = readLineIterator.of[Int](_.toInt).toIndexedSeq.iterator

    // then
    intercept[java.io.IOException] {
      readLineIterator.reader.ready()
    }
    seq.toList should contain theSameElementsInOrderAs Seq(1, 2)

  }

}
