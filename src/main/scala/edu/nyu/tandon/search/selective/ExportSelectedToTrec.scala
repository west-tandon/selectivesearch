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
                      model: String = null)

    val parser = new OptionParser[Config](CommandName) {

      opt[String]('n', "basename")
        .action((x, c) => c.copy(basename = x))
        .text("the prefix of the files")
        .required()

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        logger.info(s"Exporting results at ${config.basename} to TREC format")

        TrecResults
          .fromSelected(config.basename)
          .store(config.basename)

      case None =>
    }
  }

}
