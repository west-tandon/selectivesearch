package edu.nyu.tandon.search.selective

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective.data.Properties
import edu.nyu.tandon.search.selective.data.features.Features
import edu.nyu.tandon.search.selective.data.payoff.Payoffs
import org.apache.spark.sql.{Row, SparkSession}
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

        val properties = Properties.get(config.basename)
        val features = Features.get(properties)

        val spark = SparkSession.builder().master("local").getOrCreate()
        import spark.implicits._
        val baseResults = spark.read.parquet(s"${config.basename}.results")

        for (shard <- 0 until features.shardCount) {
          spark.read.parquet(s"${config.basename}#$shard.results")
            .join(baseResults.select($"query", $"docid-global", $"ridx" as "ridx-base"),
              Seq("query", "docid-global"))
            .groupBy($"query", $"shard", $"bucket")
            .count()
            .orderBy($"query", $"shard", $"bucket")
            .withColumnRenamed("count", "impact")
            .write
            .parquet(s"${config.basename}#$shard.impacts")
        }
      case None =>
    }

  }

}
