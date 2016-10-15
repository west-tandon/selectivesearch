package edu.nyu.tandon.search.selective

import java.io.{FileWriter, Writer}

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective.data.features.Features
import scopt.OptionParser

import scalax.io.Resource

/**
  * @author michal.siedlaczek@nyu.edu
  */
object Overlap extends LazyLogging {

  val OverlapLevels: Seq[Int] = Seq( 5, 10, 20, 50, 100 )

  val CommandName = "overlap"

  def calcOverlaps(input: Iterator[Seq[Long]], reference: Iterator[Seq[Long]]): Iterator[Seq[Double]] = {
    for ((inputResults, referenceResults) <- input zip reference) yield
      for (k <- OverlapLevels) yield {
        val topKReference = referenceResults.take(k)
        inputResults.count(topKReference.contains(_)).toDouble / topKReference.length.toDouble
      }
  }

  def writeAndCalcAvgs(overlaps: Iterator[Seq[Double]], writers: Seq[Writer]): Seq[Double] = {
    overlaps.foldLeft((OverlapLevels.map(_ => 0.0), OverlapLevels.map(_ => 0))) ((acc: (Seq[Double], Seq[Int]), v: Seq[Double]) => {
      acc match {
        case (sums, counts) =>
          for ((writer, value) <- writers zip v) writer.append(value.toString).append('\n')
          (sums.zip(v).map(x => x._1 + x._2), counts.map(_ + 1))
      }
    }) match {
      case (sums, counts) => sums.zip(counts).map {
        case (sum, count) => sum / count
      }
    }
  }

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

        logger.info(s"Calculating overlap for ${config.basename}")

        val input = Load.selectedDocumentsAt(config.basename)
        val reference = Features.get(base(config.basename)).baseResults

        val overlaps = calcOverlaps(input, reference)

        val writers = for (k <- OverlapLevels) yield new FileWriter(Path.toOverlaps(config.basename, k))
        val avg = writeAndCalcAvgs(overlaps, writers)

        for (((avgOverlap, w), k) <- (avg zip writers) zip OverlapLevels) {
          logger.info(s"Overlap @$k = $avgOverlap")
          Resource.fromFile(Path.toOverlap(config.basename, k)).write(s"$avgOverlap\n")
          w.close()
        }

      case None =>
    }

  }

}
