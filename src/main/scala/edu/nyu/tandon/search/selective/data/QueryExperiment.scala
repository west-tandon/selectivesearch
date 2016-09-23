package edu.nyu.tandon.search.selective.data

import edu.nyu.tandon.search.selective.data.QueryExperiment._

import scala.io.Source

/**
  * @author michal.siedlaczek@nyu.edu
  */
class QueryExperiment(val queriesIterator: Iterator[String],
                      val payoffIterator: Iterator[String],
                      val costIterator: Iterator[String],
                      val shardIds: List[Int]) extends Iterable[QueryData] {

  def this(basename: String) = {

    this(
      Source.fromFile(basename + QueriesSuffix).getLines(),
      Source.fromFile(basename + PayoffSuffix).getLines(),
      Source.fromFile(basename + CostSuffix).getLines(),
      readDivision(Source.fromFile(basename + DivisionSuffix))
        .zipWithIndex.flatMap { case (count: Int, index) => for (i <- 0 until count) yield index }
    )
  }

  override def iterator: Iterator[QueryData] = {

    new Iterator[QueryData] {
      override def hasNext: Boolean =
        if (costIterator.hasNext || payoffIterator.hasNext || queriesIterator.hasNext) {
          require(costIterator.hasNext && payoffIterator.hasNext && queriesIterator.hasNext,
            "unexpected end of some files")
          true
        }
        else false
      override def next(): QueryData = {

        val costs = costIterator.next().split(FieldSplitter).map(_.toDouble)
        val payoffs = payoffIterator.next().split(FieldSplitter).map(_.toDouble)
        val query = queriesIterator.next()

        require(costs.length == payoffs.length, "unequal cost and payoff lists")

        val bins = shardIds zip payoffs zip costs map {
          case ((shardId, payoff), cost) => Bin(shardId, payoff, cost)
        }

        QueryData(query, bins)
      }
    }
  }

}

object QueryExperiment {

  val CostSuffix = ".cost"
  val DivisionSuffix = ".division"
  val PayoffSuffix = ".payoff"
  val QueriesSuffix = ".queries"
  val SelectionSuffix = ".selection"
  val FieldSplitter = "\\s+"
  val FieldSeparator = " "

  def readDivision(source: Source): List[Int] = {
    val lines = source.getLines()
    require(lines.nonEmpty, "division file is empty")
    lines.next()
      .split(FieldSplitter)
      .map(_.toInt)
      .toList
  }
}
