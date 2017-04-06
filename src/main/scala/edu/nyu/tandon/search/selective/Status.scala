package edu.nyu.tandon.search.selective

import java.io.{File, FileFilter}

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective.data.Properties
import org.apache.commons.io.filefilter.{RegexFileFilter, WildcardFileFilter}
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */
class Status(properties: Properties) extends LazyLogging {

  object Indicator {
    sealed trait EnumVal {
      def indicator: String
    }
    case object Present extends EnumVal {
      override def indicator: String = "+"
    }
    case object Missing extends EnumVal {
      override def indicator: String = " "
    }
    case object Warning extends EnumVal {
      override def indicator: String = "!"
    }
    case object Partial extends EnumVal {
      override def indicator: String = "."
    }
    def aggregate(indicators: Seq[Indicator.EnumVal]): Indicator.EnumVal = {
      val warnings = indicators.count(_ == Indicator.Warning)
      val missing = indicators.count(_ == Indicator.Missing)
      val present = indicators.count(_ == Indicator.Present)
      if (warnings > 0) Indicator.Warning
      else if (present == indicators.length) Indicator.Present
      else if (missing == indicators.length) Indicator.Missing
      else Indicator.Partial
    }
  }

  val bucketCount: Int = properties.bucketCount
  val shardCount: Int = properties.features.shardCount

  def reportBuckets() = {
    val impact = bucketBased("payoff")
    val cost = bucketBased("cost")
    val docrank = bucketBased("docrank")
    val all = Seq(impact, cost, docrank)
    printStatus("Buckets",
      if (all.contains(Indicator.Warning)) Indicator.Warning
      else if (all.contains(Indicator.Present)) Indicator.Present else Indicator.Missing)
    printStatus("Impact", impact, level = 1)
    printStatus("Cost", cost, level = 1)
    printStatus("Docrank", docrank, level = 1)
  }

  def bucketBased(extension: String): Indicator.EnumVal = {
    val fileCount = (for (shard <- 0 until shardCount) yield
      for (bucket <- 0 until bucketCount) yield
        new File(s"${properties.basename}#$shard#$bucket.$extension").exists()).flatten.map(if (_) 1 else 0).sum
    if (fileCount == 0) Indicator.Missing
    else if (fileCount == bucketCount * shardCount) Indicator.Present
    else Indicator.Warning
  }

  def reportFeatures() = {
    val globalResults = singleFeature("results.global")
    val resultsGlobal = shardFeature("results.global")
    val resultsLocal = shardFeature("results.local")
    val scores = shardFeature("results.scores")
    printStatus("Features", Indicator.aggregate(Seq(resultsGlobal, resultsLocal, scores)))
    printStatus("Global results", resultsGlobal, level = 1)
    printStatus("Shard results (global IDs)", resultsGlobal, level = 1)
    printStatus("Shard results (local IDs)", resultsLocal, level = 1)
    printStatus("Shard scores", scores, level = 1)
  }

  def shardFeature(extension: String): Indicator.EnumVal = {
    val fileCount = (for (shard <- 0 until shardCount) yield
      new File(s"${properties.features.basename}#$shard.$extension").exists()).map(if (_) 1 else 0).sum
    if (fileCount == 0) Indicator.Missing
    else if (fileCount == shardCount) Indicator.Present
    else Indicator.Warning
  }

  def singleFeature(extension: String): Indicator.EnumVal =
    if (new File(s"${properties.features.basename}.$extension").exists()) Indicator.Present
    else Indicator.Missing

  def report():  Unit = {
    reportFeatures()
    reportBuckets()
  }

  def printStatus(stage: String, indicator: Indicator.EnumVal, level: Int = 0): Unit = {
    println(s"${List.fill(level)("\t").mkString}[${indicator.indicator}]\t$stage")
  }

}

object Status extends LazyLogging {

  val CommandName = "status"
  
  def getProperties(path: File): Properties = {
    assert(path.exists(), s"${path.getAbsolutePath} does not exist")
    if (path.isFile) {
      new Properties(path.getAbsolutePath)
    }
    else {
      val propertyFiles = path.list.filter(_.endsWith(".properties"))
      assert(propertyFiles.length > 0, "no *.properties file found")
      assert(propertyFiles.length < 2, s"multiple *.properties file found: ${propertyFiles.mkString(", ")}")
      new Properties(propertyFiles(0))
    }
  }

  def printBoolStatus(stage: String, status: Boolean, level: Int = 0): Unit = {
    println(s"${List.fill(level)("\t").mkString}[${if (status) "*" else " "}]\t$stage")
  }

  def main(args: Array[String]): Unit = {

    case class Config(path: File = new File("."))

    val parser = new OptionParser[Config](CommandName) {

      opt[File]('p', "path")
        .action((x, c) => c.copy(path = x))
        .text("Directory (current in directory by default) or property file")

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        try {
          val properties = getProperties(config.path)
          val status = new Status(properties)
          status.report()
        } catch {
          case e: AssertionError => logger.error(e.getMessage)
        }

      case None =>
    }

  }

}
