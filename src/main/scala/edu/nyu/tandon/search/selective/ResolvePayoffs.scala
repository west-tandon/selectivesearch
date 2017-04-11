package edu.nyu.tandon.search.selective

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective.data.Properties
import edu.nyu.tandon.search.selective.data.features.Features
import org.apache.spark.sql.functions.{sum, when}
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */
object ResolvePayoffs extends LazyLogging {

  val CommandName = "resolve-payoffs"

  def main(args: Array[String]): Unit = {

    case class Config(basename: String = null,
                      k: Int = Int.MaxValue)

    val parser = new OptionParser[Config](CommandName) {

      arg[String]("<basename>")
        .action((x, c) => c.copy(basename = x))
        .text("the prefix of the files")
        .required()

      opt[Int]('k', "at")
        .action((x, c) => c.copy(k = x))

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        logger.info(s"Resolving payoffs for ${config.basename}")

        val properties = Properties.get(config.basename)
        val features = Features.get(properties)

        val spark = SparkSession.builder().master("local").getOrCreate()
        import spark.implicits._
        val baseResults = spark.read.parquet(s"${features.basename}.results")

        for (shard <- 0 until properties.shardCount) {
          spark.read.parquet(s"${features.basename}#$shard.results-${properties.bucketCount}")
            .join(baseResults.select($"query", $"gdocid", $"rank" as "rank-base"),
              Seq("query", "gdocid"),
              "leftouter")
            .withColumn("y", when($"rank-base".isNull or ($"rank-base" >= config.k), 0).otherwise(1))
            .groupBy($"query", $"shard", $"bucket")
            .agg(sum("y").cast(FloatType).as("impact"))
            .orderBy($"query", $"shard", $"bucket")
            .write
            .mode(SaveMode.Overwrite)
            .parquet(s"${config.basename}#$shard.impacts")
        }
      case None =>
    }

  }

}
