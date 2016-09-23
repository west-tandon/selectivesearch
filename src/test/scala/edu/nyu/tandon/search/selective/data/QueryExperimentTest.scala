package edu.nyu.tandon.search.selective.data

import org.scalatest.FunSuite

import scala.io.Source

/**
  * @author michal.siedlaczek@nyu.edu
  */
class QueryExperimentTest extends FunSuite {

  trait Experiment {
    val queryExperiment = new QueryExperiment(getClass.getResource("/").getPath + "test")
  }

  test("readDivision") {
    // given
    val divisionSource = Source.fromURL(getClass.getResource("/test.division"))
    // then
    assert(QueryExperiment.readDivision(divisionSource) == Seq(3, 3, 3))
  }

  test("iterator") {
    new Experiment {
      val it = queryExperiment.iterator

      assert(it.hasNext === true)
      assert(it.toList === List(
        QueryData("query one", List(
          Bin(0, 1, 1),
          Bin(0, 0, 1),
          Bin(0, 0, 1),
          Bin(1, 3, 1),
          Bin(1, 2, 1),
          Bin(1, 1, 1),
          Bin(2, 2, 1),
          Bin(2, 1, 1),
          Bin(2, 0, 1)
        )),
        QueryData("query two", List(
          Bin(0, 7, 1),
          Bin(0, 3, 1),
          Bin(0, 0, 1),
          Bin(1, 0, 1),
          Bin(1, 0, 1),
          Bin(1, 0, 1),
          Bin(2, 0, 1),
          Bin(2, 0, 1),
          Bin(2, 0, 1)
        )),
        QueryData("query three", List(
          Bin(0, 2, 1),
          Bin(0, 1, 1),
          Bin(0, 0, 1),
          Bin(1, 2, 1),
          Bin(1, 1, 1),
          Bin(1, 1, 1),
          Bin(2, 2, 1),
          Bin(2, 1, 1),
          Bin(2, 0, 1)
        ))
      ))
    }
  }

}
