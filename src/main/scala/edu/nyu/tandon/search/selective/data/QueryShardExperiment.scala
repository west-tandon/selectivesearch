package edu.nyu.tandon.search.selective.data

import java.io.FileInputStream
import java.util.Properties
import edu.nyu.tandon._
import edu.nyu.tandon.search.selective._


import scala.io.{BufferedSource, Source}

/**
  * @author michal.siedlaczek@nyu.edu
  */
class QueryShardExperiment(val querySource: BufferedSource,
                           val payoffSources: Seq[Seq[BufferedSource]],
                           val costSources: Seq[Seq[BufferedSource]],
                           val properties: Properties) extends Iterable[QueryData] {

  override def iterator: Iterator[QueryData] = {

    /* Open iterators */
    val queries = querySource.getLines()
    val payoffs = payoffSources map (_.map(_.getLines()))
    val costs = costSources map (_.map(_.getLines()))

    new Iterator[QueryData] {

      override def hasNext: Boolean = {
        def allPayoffsHaveNext = payoffs map (_.map(_.hasNext).reduce(_ && _)) reduce (_ && _)
        def allCostsHaveNext = costs map (_.map(_.hasNext).reduce(_ && _)) reduce (_ && _)
        if (queries.hasNext && allPayoffsHaveNext && allCostsHaveNext) true
        else {
          def noPayoffHasNext = payoffs map (_.map(!_.hasNext).reduce(_ && _)) reduce (_ && _)
          def noCostHasNext = costs map (_.map(!_.hasNext).reduce(_ && _)) reduce (_ && _)
          require(!queries.hasNext && noPayoffHasNext && noCostHasNext,
            "unexpected end of some files")
          false
        }
      }

      override def next(): QueryData = {

        val nextQuery = queries.next()

        val nextPayoffs: Seq[List[Double]] = payoffs map (_.map(_.next().toDouble).toList)
        val nextCosts:   Seq[List[Double]] = costs map (_.map(_.next().toDouble).toList)

        val binsByShard = (nextPayoffs zip nextCosts).zipWithIndex map {
          case ((pl, cl), shardId) => (pl zip cl) map { case (p, c) => Bin(shardId, p, c) }
        }

        new QueryData(nextQuery, binsByShard)
      }

    }

  }

  def numberOfShards: Int = properties.getProperty("shards.count").toInt

}

object QueryShardExperiment {

  def fromBasename(basename: String): QueryShardExperiment = {

    val properties = new Properties()
    properties.load(new FileInputStream(s"$basename$PropertiesSuffix"))
    val shardCount = properties.getProperty("shards.count").toInt
    val binCount = properties.getProperty("bins.count").toInt

    new QueryShardExperiment(
      Source.fromFile(s"$basename$QueriesSuffix"),
      for (i <- 0 until shardCount) yield
        for (b <- 0 until binCount) yield Source.fromFile(s"$basename#$i#$b$PayoffSuffix"),
      for (i <- 0 until shardCount) yield
        for (b <- 0 until binCount) yield Source.fromFile(s"$basename#$i#$b$CostSuffix"),
      properties
    )

  }

}
