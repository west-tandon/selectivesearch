package edu.nyu.tandon.search.selective

import edu.nyu.tandon.search.selective.data.Properties
import edu.nyu.tandon.search.selective.data.features.Features
import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */
object QRels2Parquet {

  val CommandName = "qrels2parquet"

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

        val spark = SparkSession.builder().master("local").getOrCreate()
        import spark.implicits._

        val properties = Properties.get(config.basename)
        val features = Features.get(properties)

        val qrels = features.qrelsReference

        val rows = for ((rels, query: Int) <- qrels.zipWithIndex) yield {
          for (docidGlobal <- rels) yield {
            (query, docidGlobal.toLong)
          }
        }

        rows.flatten.toDF("query", "gdocid")
          .write
          .mode(SaveMode.Overwrite)
          .parquet(s"${features.basename}.relevance")

      case None =>
    }

  }

}
