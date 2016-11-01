package edu.nyu.tandon.search.selective.data.learn

import edu.nyu.tandon.search.selective.learn.TrainCosts
import edu.nyu.tandon.test.BaseFunSuite
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row
import org.scalatest.Matchers._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class TrainCostsTest extends BaseFunSuite {

  test("dataFrameFromBasename") {
    val actual = TrainCosts.trainingDataFromBasename(s"$resourcesPath/test").collectAsList()
    val expected = List(
      Row(Vectors.dense(2.0, 200, 199, 100, 101, 2000), 3.0),
      Row(Vectors.dense(2.0, 200, 199, 100, 101, 2000), 3.0),
      Row(Vectors.dense(2.0, 200, 199, 100, 101, 2000), 3.0),

      Row(Vectors.dense(2.0, 100, 99, 0, 1, 1000), 3.0),
      Row(Vectors.dense(2.0, 100, 99, 0, 1, 1000), 3.0),
      Row(Vectors.dense(2.0, 100, 99, 0, 1, 1000), 3.0)
    )

    actual should contain theSameElementsAs expected
  }

}
