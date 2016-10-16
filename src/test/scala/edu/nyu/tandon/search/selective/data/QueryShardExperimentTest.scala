package edu.nyu.tandon.search.selective.data

import org.scalatest.FunSuite

/**
  * @author michal.siedlaczek@nyu.edu
  */
class QueryShardExperimentTest extends FunSuite {

  trait Experiment {
    val queryExperiment = QueryShardExperiment.fromBasename(getClass.getResource("/").getPath + "test")
  }

  test("iterator") {
    new Experiment {
      val it = queryExperiment.iterator

      assert(it.hasNext === true)
      assert(it.toList === List(
        QueryData(Seq(
          List(Bucket(0, 1, 1), Bucket(0, 0, 1), Bucket(0, 0, 1)),
          List(Bucket(1, 3, 1), Bucket(1, 2, 1), Bucket(1, 1, 1)),
          List(Bucket(2, 2, 1), Bucket(2, 1, 1), Bucket(2, 0, 1))
        )),
        QueryData(Seq(
          List(Bucket(0, 3, 1), Bucket(0, 3, 1), Bucket(0, 0, 1)),
          List(Bucket(1, 0, 1), Bucket(1, 0, 1), Bucket(1, 0, 1)),
          List(Bucket(2, 0, 1), Bucket(2, 0, 1), Bucket(2, 0, 1))
        ))
      ))
      assert(it.hasNext === false)
    }
  }

}
