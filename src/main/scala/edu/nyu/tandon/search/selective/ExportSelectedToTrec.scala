package edu.nyu.tandon.search.selective

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective.data.results.trec.TrecResults
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */
object ExportSelectedToTrec extends LazyLogging {

  val CommandName = "export-trec"

  def main(args: Array[String]): Unit = {

    case class Config(basename: String = null,
                      input: String = null)

    val parser = new OptionParser[Config](CommandName) {

      arg[String]("<basename>")
        .action((x, c) => c.copy(basename = x))
        .text("the prefix of the files")
        .required()

      opt[String]('i', "input")
        .action((x, c) => c.copy(input = x))
        .text("the input to be exported")

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        logger.info(s"Exporting results at ${config.basename} to TREC format")

        val input = if (config.input == null) config.basename else config.input

        TrecResults
          .fromSelected(config.basename, input)
          .store(config.basename)

      case None =>
    }
  }

}
