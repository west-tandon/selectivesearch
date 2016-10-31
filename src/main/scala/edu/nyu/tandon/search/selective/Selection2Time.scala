package edu.nyu.tandon.search.selective

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective.data.Properties
import edu.nyu.tandon.search.selective.data.features.Features
import edu.nyu.tandon.utils.Lines
import edu.nyu.tandon.utils.Lines._
import edu.nyu.tandon.utils.WriteLineIterator._
import scopt.OptionParser

import scalax.io.Resource

/**
  * @author michal.siedlaczek@nyu.edu
  */
object Selection2Time extends LazyLogging {

  val CommandName = "selection2time"

  def main(args: Array[String]): Unit = {

    case class Config(basename: String = null)

    val parser = new OptionParser[Config](CommandName) {

      arg[String]("<basename>")
        .action((x, c) => c.copy(basename = x))
        .text("the prefix of the files")
        .required()

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        logger.info(s"Converting selection to time at ${config.basename}")

        val selectionIterator = Lines.fromFile(s"${config.basename}.selection").ofSeq[Int]

        val properties = Properties.get(config.basename)
        val times = Features.get(properties).times
        val bucketCount = properties.bucketCount

        val selectionTimes = for ((selection, shardTimes) <- selectionIterator zip times) yield
          selection.zip(shardTimes).map {
            case (bucketsInShards, totalTime) => totalTime * (bucketsInShards.toDouble / bucketCount.toDouble)
          }.sum

        val (sum, count) = selectionTimes.aggregating.write(s"${config.basename}.selection.time")
        Resource.fromFile(s"${config.basename}.selection.time.avg").write(String.valueOf(sum / count.toDouble))

      case None =>
    }

  }

}
