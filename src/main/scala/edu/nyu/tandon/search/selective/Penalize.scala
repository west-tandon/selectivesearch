package edu.nyu.tandon.search.selective

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective.data.Properties
import edu.nyu.tandon.utils.Lines
import edu.nyu.tandon.utils.Lines._
import edu.nyu.tandon.utils.WriteLineIterator._
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */
object Penalize extends LazyLogging {

  val CommandName = "penalize"

  def main(args: Array[String]): Unit = {

    case class Config(basename: String = null,
                      penalty: Double = 0)

    val parser = new OptionParser[Config](CommandName) {

      arg[String]("<basename>")
        .action((x, c) => c.copy(basename = x))
        .text("the prefix of the files")
        .required()

      opt[Double]('p', "penalty")
        .action((x, c) => c.copy(penalty = x))
        .text("the penalty to add to every first bucket of a shard")
        .required()

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        logger.info(s"Penalizing ${config.basename}")

        val shardCount = Properties.get(config.basename).features.shardCount

        for (shard <- 0 until shardCount) {
          val file = Path.toCosts(config.basename, shard, 0)
          Lines.fromFile(file).of[Double]
            .map(_ + config.penalty)
            .write(file)
        }

      case None =>
    }

  }

}
