package edu.nyu.tandon.search.selective

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.loadProperties
import edu.nyu.tandon.search.selective.data.features.Features
import edu.nyu.tandon.search.selective.data.results.{FlatResults, _}
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */
object BucketizeResults extends LazyLogging {

  val CommandName = "bucketize"

  def parseBasename(basename: String): (String, Option[Int]) = {
    val s = basename.split(NestingIndicator)
    if (s.length == 1) (basename, None)
    else if (s.length == 2) (s(0), Some(s(1).toInt))
    else throw new IllegalStateException("Found multiple # in shard-level file")
  }

  def main(args: Array[String]): Unit = {

    case class Config(basename: String = null)

    val parser = new OptionParser[Config](CommandName) {

      opt[String]('n', "basename")
        .action((x, c) => c.copy(basename = x))
        .text("the prefix of the files")
        .required()

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        val (basename: String, shard: Option[Int]) = parseBasename(config.basename)

        val properties = loadProperties(basename)
        val bucketCount = properties.getProperty("buckets.count").toInt
        val features = Features.get(basename)
        val shardCount = features.shardCount

        shard match {
          case Some(shardId) =>

            val bucketSize = math.ceil(features.shardSize(shardId).toDouble / bucketCount.toDouble).toLong

            logger.info(s"Bucketizing shard $shardId with bucket size $bucketSize")

            FlatResults
              .fromFeatures(features, shardId)
              .bucketize(bucketSize, bucketCount)
              .store(config.basename)

          case None =>

//            val bucketSizes = for (shardId <- 0 until shardCount) yield
//              math.ceil(properties.getProperty(s"maxId.$shardId").toDouble / bucketCount.toDouble).toLong

            val bucketSizes = features.shardSizes.map(
              (shardSize) => math.ceil(shardSize.toDouble / bucketCount.toDouble).toLong
            )

            logger.info(s"Bucketizing all $shardCount shards with bucket sizes: ${bucketSizes.mkString(" ")}")

            resultsByShardsFromBasename(basename)
              .bucketize(bucketSizes, bucketCount)
              .store(basename)

        }

      case None =>
    }

  }

}
