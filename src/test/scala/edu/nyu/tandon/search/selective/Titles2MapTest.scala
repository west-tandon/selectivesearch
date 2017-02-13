package edu.nyu.tandon.search.selective

import java.io.{FileInputStream, ObjectInputStream}

import edu.nyu.tandon.test.BaseFunSuite
import edu.nyu.tandon.utils.WriteLineIterator._
import org.scalatest.Matchers._

import scala.collection.immutable.HashMap

/**
  * @author michal.siedlaczek@nyu.edu
  */
class Titles2MapTest extends BaseFunSuite {

  trait T {
    val tmpDir = createTemporaryDir()
    val basename = "basename"
    val features = "features"

    List("features = features",
      "buckets.count = 4",
      "k = 4").write(s"$tmpDir/$basename.properties")

    List(
      "a",
      "b",
      "c",
      "d",
      "e"
    ).write(s"$tmpDir/$features.titles")
  }

  test("main") {
    new T {
      Titles2Map.main(Array(s"$tmpDir/$basename"))
      val ois = new ObjectInputStream(new FileInputStream(s"$tmpDir/$features.titlemap"))
      val map = ois.readObject().asInstanceOf[HashMap[String, Int]]
      map should contain theSameElementsAs List(
        ("a", 0),
        ("b", 1),
        ("c", 2),
        ("d", 3),
        ("e", 4)
      )
    }
  }

}
