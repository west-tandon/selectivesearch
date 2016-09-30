package edu.nyu.tandon.search.selective

import java.io.{BufferedWriter, FileWriter}

import edu.nyu.tandon.loadProperties
import scopt.OptionParser

import scala.io.Source

/**
  * @author michal.siedlaczek@nyu.edu
  */
object BinnifyResults {

  def parseBasename(basename: String): (String, Option[Int]) = {
    val s = basename.split(NestingIndicator)
    if (s.length == 1) (basename, None)
    else if (s.length == 2) (s(0), Some(s(1).toInt))
    else throw new IllegalStateException("Found multiple # in shard-level file")
  }

  def groupByBins(results: Array[Long], binSize: Long, binCount: Int): Seq[Array[Long]] = {
    val m = results.groupBy(_ / binSize).withDefaultValue(Array())
    for (i <- 0 until binCount) yield m(i)
  }

  def binnifyShard(source: Source, binCount: Int, maxId: Long): Iterator[Seq[Array[Long]]] = {

    val binSize = math.ceil(maxId.toDouble / binCount.toDouble).toLong

    val lines = source.getLines()
    val flatResults = lines.map(_.split(FieldSplitter).map(_.toLong))
    flatResults.map(groupByBins(_, binSize, binCount))

  }

  def binnifyShard(basename: String, shardId: Int, binCount: Int, maxId: Long): Iterator[Seq[Array[Long]]] =
    binnifyShard(Source.fromFile(s"$basename#$shardId$ResultsSuffix"),
      binCount, maxId)

  def binnify(basename: String, shardCount: Int, binCount: Int, maxId: Long): Seq[Iterator[Seq[Array[Long]]]] =
    for (i <- 0 until shardCount) yield binnifyShard(basename, i, binCount, maxId)

  def writeBinnified(basename: String, shardCount: Int, binCount: Int, maxId: Long): Unit =
    for (i <- 0 until shardCount)
      writeBinnifiedShard(s"$basename#$i", binCount, binnifyShard(s"$basename", i, binCount, maxId))

  def writeBinnifiedShard(basename: String, binCount: Int, binnifiedShards: Iterator[Seq[Array[Long]]]) = {
    val writers = for (i <- 0 until binCount) yield new BufferedWriter(new FileWriter(s"$basename#$i$ResultsSuffix"))
    for (binnedResultsForQuery <- binnifiedShards)
      for ((writer, resultsForBin) <- (writers, binnedResultsForQuery).zipped) {
        writer.append(resultsForBin.mkString(FieldSeparator))
        writer.newLine()
      }
    for (writer <- writers) writer.close()
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
        val shardCount = properties.getProperty("shards.count").toInt
        val binCount = properties.getProperty("bins.count").toInt
        val maxId = properties.getProperty("maxId").toLong

        shard match {
          case Some(shardId) =>
            writeBinnifiedShard(config.basename, binCount,
              binnifyShard(basename, shardId, binCount, maxId))
          case None          =>
            writeBinnified(basename, shardCount, binCount, maxId)
        }

      case None =>
    }

  }

}
