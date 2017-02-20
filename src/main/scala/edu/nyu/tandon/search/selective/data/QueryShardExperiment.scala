package edu.nyu.tandon.search.selective.data

import edu.nyu.tandon.search.selective._
import edu.nyu.tandon.search.selective.data.features.Features

import scala.io.{BufferedSource, Source}

/**
  * @author michal.siedlaczek@nyu.edu
  */
class QueryShardExperiment(val payoffs: Seq[Seq[Iterator[String]]],
                           val costs: Seq[Seq[Iterator[String]]],
                           val features: Features) extends Iterable[QueryData] {

  def numberOfShards: Int = features.shardCount

  override def iterator: Iterator[QueryData] = {

    /* Open iterators */
//    val payoffs = payoffSources map (_.map(_.getLines().toStream.iterator))
//    val costs = costSources map (_.map(_.getLines().toStream.iterator))

    new Iterator[QueryData] {

      override def hasNext: Boolean = {
        def allPayoffsHaveNext = payoffs map (_.map(_.hasNext).reduce(_ && _)) reduce (_ && _)
        def allCostsHaveNext = costs map (_.map(_.hasNext).reduce(_ && _)) reduce (_ && _)
        if (allPayoffsHaveNext && allCostsHaveNext) true
        else {
          def noPayoffHasNext = payoffs map (_.map(!_.hasNext).reduce(_ && _)) reduce (_ && _)
          def noCostHasNext = costs map (_.map(!_.hasNext).reduce(_ && _)) reduce (_ && _)
          require(noPayoffHasNext && noCostHasNext,
            "unexpected end of some files")
          false
        }
      }

      override def next(): QueryData = {

        val nextPayoffs: Seq[List[Double]] = payoffs map (_.map(_.next().toDouble).toList)
        val nextCosts:   Seq[List[Double]] = costs map (_.map(_.next().toDouble).toList)

        val bucketsByShard = (nextPayoffs zip nextCosts).zipWithIndex map {
          case ((pl, cl), shardId) => (pl zip cl) map { case (p, c) => Bucket(shardId, p, c) }
        }

        new QueryData(bucketsByShard)
      }

    }

  }

}

object QueryShardExperiment {

  def fromBasename(basename: String): QueryShardExperiment = {

    val properties = Properties.get(basename)
    val shardCount = Features.get(properties).shardCount
    val bucketCount = properties.bucketCount

    new QueryShardExperiment(
      for (s <- 0 until shardCount) yield
        for (b <- 0 until bucketCount) yield Source.fromFile(Path.toPayoffs(basename, s, b)).getLines().toStream.iterator,
      for (s <- 0 until shardCount) yield
        for (b <- 0 until bucketCount) yield Source.fromFile(Path.toCosts(basename, s, b)).getLines().toStream.iterator,
      Features.get(basename)
    )

  }

}
