package edu.nyu.tandon.search.selective

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective.data.Properties
import edu.nyu.tandon.search.selective.data.features.Features
import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.when
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */
object LabelResults extends LazyLogging {

  val CommandName = "label-results"

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

        val properties = Properties.get(config.basename)
        val features = Features.get(properties)
        val spark = SparkSession.builder().master("local").getOrCreate()
        import spark.implicits._

        val baseResults = spark.read.parquet(s"${features.basename}.results")
        val relevantResults = spark.read.parquet(s"${features.basename}.relevance")
        for (shard <- 0 until properties.shardCount) {

          logger.info(s"processing shard $shard")

          val shardResults = spark.read.parquet(s"${features.basename}#$shard.results-${properties.bucketCount}")

          val labeledResults = shardResults
            .join(baseResults.select($"query", $"docid-global", $"ridx" as "base-ridx"), Seq("query", "docid-global"), "leftouter")
            .join(relevantResults.select($"query", $"docid-global", $"docid-global" as "relevant-indicator"), Seq("query", "docid-global"), "leftouter")
            .withColumn("relevant", when($"relevant-indicator".isNotNull, true).otherwise(false))
            .withColumn("baseorder", when($"base-ridx".isNotNull, $"base-ridx").otherwise(Int.MaxValue))

          val columns = shardResults.columns ++ Array("relevant", "baseorder")
          labeledResults.select(columns.head, columns.drop(1):_*)
            .orderBy("query", "bucket", "ridx")
            .write
            .mode(SaveMode.Overwrite)
            .parquet(s"${features.basename}#$shard.labeledresults-${properties.bucketCount}")

        }

      case None =>
    }

  }
}
