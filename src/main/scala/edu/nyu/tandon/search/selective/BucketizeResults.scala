package edu.nyu.tandon.search.selective

import java.io.FileWriter

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon._
import edu.nyu.tandon.search.selective.data.features.Features
import edu.nyu.tandon.search.selective.data.results.FlatResults
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

    case class Config(basename: String = null,
                      bucketizeCosts: Boolean = true)

    val parser = new OptionParser[Config](CommandName) {

      arg[String]("<basename>")
        .action((x, c) => c.copy(basename = x))
        .text("the prefix of the files")
        .required()

      opt[Boolean]('c', "cost")
        .action((x, c) => c.copy(bucketizeCosts = x))
        .text("whether to bucketize costs (requires costs among input files)")

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        val (basename: String, shard: Option[Int]) = parseBasename(config.basename)

        val properties = loadProperties(basename)
        val bucketCount = properties.getProperty("buckets.count").toInt
        val k = properties.getProperty("k").toInt
        val features = Features.get(basename)
        val shardCount = features.shardCount

        shard match {
          case Some(shardId) =>

            val bucketSize = math.ceil(features.shardSize(shardId).toDouble / bucketCount.toDouble).toLong

            logger.info(s"Bucketizing shard $shardId with bucket size $bucketSize")

            FlatResults
              .fromFeatures(features, shardId, k)
              .bucketize(bucketSize, bucketCount)
              .store(config.basename)

            if (config.bucketizeCosts) bucketizeCosts(config.basename, features, shardId, bucketCount)

          case None =>

            val bucketSizes = features.shardSizes.map(
              (shardSize) => math.ceil(shardSize.toDouble / bucketCount.toDouble).toLong
            )

            logger.info(s"Bucketizing all $shardCount shards with bucket sizes: ${bucketSizes.mkString(" ")}")

            for ((bucketSize, shardId) <- bucketSizes.zipWithIndex) {
              logger.info(s"Bucketizing shard $shardId with bucket size $bucketSize")

              FlatResults
                .fromFeatures(features, shardId, k)
                .bucketize(bucketSize, bucketCount)
                .store(s"${config.basename}#$shardId")

              if (config.bucketizeCosts) bucketizeCosts(s"${config.basename}#$shardId", features, shardId, bucketCount)
            }

        }

      case None =>
    }

  }

  def bucketizeCosts(basename: String, features: Features, shardId: Int, bucketCount: Int): Unit = {
    val writers = for (b <- 0 until bucketCount) yield new FileWriter(basename)
    for (c <- features.costs(shardId)) {
      val unitCost = c.toDouble / bucketCount.toDouble;
      for (w <- writers) w.append(s"$unitCost\n")
    }
    for (w <- writers) w.close()
  }

}
