package edu.nyu.tandon.search.selective

import java.io.{ByteArrayOutputStream, File, PrintStream}

import edu.nyu.tandon.test.BaseFunSuite
import org.scalatest.Matchers._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class StatusTest extends BaseFunSuite {

  test("properties") {
    val properties = Status.getProperties(new File(resourcesPath + "test.properties"))
    properties.basename shouldBe resourcesPath + "test"
  }

  test("printBoolStatus") {
    val stream = new ByteArrayOutputStream()
    Console.withOut(stream) {
      Status.printBoolStatus("stage", status = true)
      Status.printBoolStatus("substage", status = true, level = 1)
      Status.printBoolStatus("substage", status = false, level = 2)
      Status.printBoolStatus("stage2", status = false)
      stream.toString shouldBe Seq(
        "[*]\tstage\n",
        "\t[*]\tsubstage\n",
        "\t\t[ ]\tsubstage\n",
        "[ ]\tstage2\n"
      ).mkString
    }
  }

}
