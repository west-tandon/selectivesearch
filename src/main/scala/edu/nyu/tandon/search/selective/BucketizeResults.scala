package edu.nyu.tandon.search.selective

import edu.nyu.tandon.loadProperties
import edu.nyu.tandon.search.selective.data.results.{FlatResults, _}
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */
object BucketizeResults {

  def parseBasename(basename: String): (String, Option[Int]) = {
    val s = basename.split(NestingIndicator)
    if (s.length == 1) (basename, None)
    else if (s.length == 2) (s(0), Some(s(1).toInt))
    else throw new IllegalStateException("Found multiple # in shard-level file")
  }

  def main(args: Array[String]): Unit = {

    case class Config(basename: String = null)

    val parser = new OptionParser[Config](this.getClass.getSimpleName) {

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
        val maxId = properties.getProperty("maxId").toLong

        shard match {
          case Some(shardId) =>
            FlatResults
              .fromBasename(config.basename)
              .partition(math.ceil(maxId.toDouble / bucketCount.toDouble).toLong, bucketCount)
              .store(config.basename)
          case None          =>
            resultsByShardsFromBasename(basename)
              .partition(math.ceil(maxId.toDouble / bucketCount.toDouble).toLong, bucketCount)
              .store(basename)
        }

      case None =>
    }

  }

}
