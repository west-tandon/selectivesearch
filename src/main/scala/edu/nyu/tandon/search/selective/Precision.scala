package edu.nyu.tandon.search.selective

import java.io.FileWriter

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.utils.Lines
import edu.nyu.tandon.utils.Lines._
import scopt.OptionParser

import scalax.io.StandardOpenOption._

/**
  * @author michal.siedlaczek@nyu.edu
  */
object Precision extends LazyLogging {

  val CommandName = "precision"

  def precision(input: Iterator[Seq[Long]], reference: Iterator[Seq[Long]])(at: Seq[Int]): Iterator[Seq[Double]] =
    for ((inputResults, referenceResults) <- input zip reference) yield
      for (k <- at) yield
        inputResults.take(k).count(referenceResults.contains(_)).toDouble / k

  def main(args: Array[String]): Unit = {

    case class Config(basename: String = null,
                      refFile: String = null,
                      at: Seq[Int] = Seq(10, 30))

    val parser = new OptionParser[Config](CommandName) {

      arg[String]("<basename>")
        .action((x, c) => c.copy(basename = x))
        .text("the prefix of the files")
        .required()

      arg[String]("<reference>")
        .action((x, c) => c.copy(refFile = x))
        .text("a file containing, for each query, a set of relevant documents")
        .required()

      opt[Seq[Int]]('k', "at")
        .action((x, c) => c.copy(at = x))
        .text("a list of k's for which to calculate P@k")
        .required()

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        logger.info(s"Calculating precision for ${config.basename} with reference ${config.refFile}")

        val input = Load.selectedDocumentsAt(config.basename)
        val reference = Lines.fromFile(config.refFile).ofSeq[Long]

        val precisionIterator = precision(input, reference)(config.at)

        val writers = for (k <- config.at) yield new FileWriter(Path.toPrecisions(config.basename, k))
        val avg = Overlap.writeAndCalcAvgs(precisionIterator, writers)

        for (((avgP, w), k) <- (avg zip writers) zip config.at) {
          logger.info(s"Precision @$k = $avgP")
          scalax.file.Path.fromString(Path.toPrecision(config.basename, k)).outputStream(WriteTruncate:_*)
            .write(s"$avgP\n")
          w.close()
        }

      case None =>
    }

  }

}
