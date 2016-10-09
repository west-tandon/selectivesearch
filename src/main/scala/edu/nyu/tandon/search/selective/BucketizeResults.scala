package edu.nyu.tandon.search.selective

import edu.nyu.tandon.loadProperties
import edu.nyu.tandon.search.selective.data.results.{FlatResults, _}
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */
object BucketizeResults {

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
        val shardCount = properties.getProperty("shards.count").toInt

        shard match {
          case Some(shardId) =>
            val bucketSize = math.ceil(properties.getProperty(s"maxId.$shardId").toDouble / bucketCount.toDouble).toLong
            FlatResults
              .fromBasename(config.basename)
              .bucketize(bucketSize, bucketCount)
              .store(config.basename)
          case None =>
            val bucketSizes = for (shardId <- 0 until shardCount) yield
              math.ceil(properties.getProperty(s"maxId.$shardId").toDouble / bucketCount.toDouble).toLong
            resultsByShardsFromBasename(basename)
              .bucketize(bucketSizes)
              .store(basename)
        }

      case None =>
    }

  }

}
