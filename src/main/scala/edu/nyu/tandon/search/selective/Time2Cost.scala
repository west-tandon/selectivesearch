package edu.nyu.tandon.search.selective

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective.data.Properties
import edu.nyu.tandon.search.selective.data.features.Features
import edu.nyu.tandon.utils.WriteLineIterator._
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */
object Time2Cost extends LazyLogging {

  val CommandName = "time2cost"

  def main(args: Array[String]): Unit = {

    case class Config(basename: String = null,
                      unitCost: Option[Double] = None)

    val parser = new OptionParser[Config](CommandName) {

      arg[String]("<basename>")
        .action((x, c) => c.copy(basename = x))
        .text("the prefix of the files")
        .required()

      opt[Double]('u', "unit-cost")
        .action((x, c) => c.copy(unitCost = Some(x)))
        .text("unit cost")

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        val properties = Properties.get(config.basename)
        val features = Features.get(properties)
        val unitCost = config.unitCost match {
          case Some(c) => c
          case None => features.avgTime
        }

        for (shard <- 0 until features.shardCount)
          features.times(shard).map(_ / unitCost).write(Path.toCosts(features.basename, shard))

      case None =>
    }

  }

}
