package edu.nyu.tandon.search.selective.data.learn

import edu.nyu.tandon.search.selective.learn.LearnPayoffs
import edu.nyu.tandon.test.BaseFunSuite
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row
import org.scalatest.Matchers._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class LearnPayoffsTest extends BaseFunSuite {

//  test("dataFrameFromBasename") {
//    val actual = LearnPayoffs.trainingDataFromBasename(s"$resourcesPath/test").collectAsList()
//    val expected = List(
//      Row(Vectors.dense(2.0, 0.1, 0.001, 0.0), 1.0),
//      Row(Vectors.dense(2.0, 0.1, 0.001, 1.0), 0.0),
//      Row(Vectors.dense(2.0, 0.1, 0.001, 2.0), 0.0),
//      Row(Vectors.dense(2.0, 0.5, 0.005, 0.0), 3.0),
//      Row(Vectors.dense(2.0, 0.5, 0.005, 1.0), 3.0),
//      Row(Vectors.dense(2.0, 0.5, 0.005, 2.0), 0.0),
//
//      Row(Vectors.dense(2.0, 0.1, 0.001, 0.0), 3.0),
//      Row(Vectors.dense(2.0, 0.1, 0.001, 1.0), 2.0),
//      Row(Vectors.dense(2.0, 0.1, 0.001, 2.0), 1.0),
//      Row(Vectors.dense(2.0, 0.5, 0.005, 0.0), 0.0),
//      Row(Vectors.dense(2.0, 0.5, 0.005, 1.0), 0.0),
//      Row(Vectors.dense(2.0, 0.5, 0.005, 2.0), 0.0),
//
//      Row(Vectors.dense(2.0, 0.1, 0.001, 0.0), 2.0),
//      Row(Vectors.dense(2.0, 0.1, 0.001, 1.0), 1.0),
//      Row(Vectors.dense(2.0, 0.1, 0.001, 2.0), 0.0),
//      Row(Vectors.dense(2.0, 0.5, 0.005, 0.0), 0.0),
//      Row(Vectors.dense(2.0, 0.5, 0.005, 1.0), 0.0),
//      Row(Vectors.dense(2.0, 0.5, 0.005, 2.0), 0.0)
//    )
//
//    actual should contain theSameElementsAs expected
//  }

}
