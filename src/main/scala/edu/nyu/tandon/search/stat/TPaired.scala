package edu.nyu.tandon.search.stat

import java.io.File

import edu.nyu.tandon.search.selective.optimize.Type._
import edu.nyu.tandon.utils.{Lines, ReadLineIterator}
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.stat.inference.TTest
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */

object TPaired {

  val CommandName = "pttest"

  def trec2values(measure: String)(trecLines: Iterator[String]): Iterator[Double] = {
    val allRegex = ".*\tall\t.*"
    val measureRegex = s"^$measure.*"
    val filteredLines = trecLines.takeWhile(!_.matches(allRegex)).filter(_.matches(measureRegex))

    val value = """.*\t.*\t(.*)""".r
    for (line <- filteredLines)
      yield line match {
        case value(valString) => valString.toDouble
      }
  }

//  /**
//    *
//    * @param measure
//    * @param result
//    * @param base
//    * @return
//    */
//  def pairedTTrec(measure: String, alpha: Double)(result: Iterator[String], base: Iterator[String]): Int = {
//    val measureConverter: Iterator[String] => Iterator[Double] = trec2values(measure)
//    pairedTTest(alpha)(measureConverter(result).toArray, measureConverter(base).toArray)
//  }

  def pairedTTest(alpha: Double)(a: Array[Double], b: Array[Double]): Int = {
    assert(a.length == b.length)
    val tTest = new TTest()
    val t = tTest.t(a, b)
    val distribution: TDistribution = new TDistribution(null, a.length - 1)
    val cumulativeP = distribution.cumulativeProbability(t)
    if (cumulativeP < alpha) -1
    else if (cumulativeP > 1 - alpha) 1
    else 0
  }

  def readSample(file: File, format: String): Iterator[Double] = {
    val lines = Lines.fromFile(file)
    format match {
      case "overlap" => lines.of[Double](_.toDouble)
      case trecMeasure => trec2values(trecMeasure)(lines)
    }
  }

  def main(args: Array[String]): Unit = {

    case class Config(sample1File: File = null,
                      sample2File: File = null,
                      sample1Format: String = null,
                      sample2Format: String = null,
                      alpha: Double = 0.01)

    val parser = new OptionParser[Config](CommandName) {

      arg[String]("<sample1_format>")
        .action((x, c) => c.copy(sample1Format = x))
        .text("Sample 1 format")
        .required()
        .text("either 'overlap' or one of the label in Trec file")

      arg[File]("<sample1_file>")
        .action((x, c) => c.copy(sample1File = x))
        .text("Sample 1")
        .required()

      arg[String]("<sample2_format>")
        .action((x, c) => c.copy(sample2Format = x))
        .text("Sample 2 format")
        .required()
        .text("either 'overlap' or one of the label in Trec file")

      arg[File]("<sample_2>")
        .action((x, c) => c.copy(sample2File = x))
        .text("Sample 2")
        .required()

      arg[Double]("<alpha>")
        .action((x, c) => c.copy(alpha = x))
        .text("level of significance")
        .required()

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        val sample1 = readSample(config.sample1File, config.sample1Format)
        val sample2 = readSample(config.sample2File, config.sample2Format)

        println(pairedTTest(config.alpha)(sample1.toArray, sample2.toArray))

      case None =>
    }
  }

}
