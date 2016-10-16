package edu.nyu.tandon.search.selective

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective.data.payoff.Payoffs
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */
object ResolvePayoffs extends LazyLogging {

  val CommandName = "resolve-payoffs"

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

        logger.info(s"Resolving payoffs for ${config.basename}")

        Payoffs
          .fromResults(config.basename)
          .store(config.basename)

      case None =>
    }

  }

}
