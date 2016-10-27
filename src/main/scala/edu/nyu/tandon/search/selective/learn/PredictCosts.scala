package edu.nyu.tandon.search.selective.learn

import java.io.FileWriter
import java.nio.file.Files

import edu.nyu.tandon._
import edu.nyu.tandon.search.selective.{Path, Spark}
import edu.nyu.tandon.search.selective.data.features.Features
import edu.nyu.tandon.search.selective.learn.TrainCosts.{FeaturesColumn, QueryColumn, ShardColumn}
import edu.nyu.tandon.utils.TupleIterators._
import edu.nyu.tandon.utils.TupleIterables._
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode._
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */
object PredictCosts {

  val CommandName = "predict-costs"

  val PredictedLabelColumn = "prediction"

  def costsFromRegressionModel(basename: String, model: RandomForestRegressionModel): Iterator[Seq[Seq[Double]]] = {
    val features = Features.get(basename)

    val shardCount = features.shardCount
    val bucketCount = loadProperties(basename).getProperty("buckets.count").toInt
    val queryCount = features.queries.length

    val featureIterator = features.queryLengths
      .zipWithIndex
      .flatZip(features.maxListLen1)
      .flatZip(features.maxListLen2)
      .flatZip(features.minListLen1)
      .flatZip(features.minListLen2)
      .flatZip(features.sumListLen)
      .flatZip(features.costs)

    val data = for ((queryId, queryLength, allMaxLL1, allMaxLL2, allMinLL1, allMinLL2, allSumLen, allCosts) <- featureIterator) yield {
      val shardFeatureIterator = allMaxLL1
        .zip(allMaxLL2)
        .flatZip(allMinLL1)
        .flatZip(allMinLL2)
        .flatZip(allSumLen)
        .flatZip(allCosts)
        .flatZipWithIndex
      for ((maxLL1, maxLL2, minLL1, minLL2, sumLen, cost, shardId) <- shardFeatureIterator)
        yield (shardId, Vectors.dense(queryLength, maxLL1, maxLL2, minLL1, minLL2, sumLen), cost)
    }

    val df = Spark.session.createDataFrame(data.flatten.toSeq)
      .withColumnRenamed("_1", QueryColumn)
      .withColumnRenamed("_2", ShardColumn)
      .withColumnRenamed("_3", FeaturesColumn)

    /* save predictions to temporary file */
    val tmp = Files.createTempDirectory(PredictedLabelColumn)
    model.transform(df).write.mode(Overwrite).save(tmp.toString)
    val predictions = Spark.session.read.parquet(tmp.toString).sort(QueryColumn, ShardColumn).collect()
    FileUtils.deleteDirectory(tmp.toFile)

    (
      for (queryId <- 0 until queryCount) yield
        for (shardId <- 0 until shardCount) yield {
          val shardCost = predictions(queryId * shardCount + shardId) match {
            case Row(q: Int, s: Int, f: Vector, cost: Double) =>
              require(q == queryId, s"expected query ID $queryId, found $q")
              require(s == shardId, s"expected query ID $shardId, found $s")
              cost
          }
          for (bucketId <- 0 until bucketCount) yield shardCost / bucketCount
        }
    ).iterator
  }

  def storeCosts(basename: String, costs: Iterator[Seq[Seq[Double]]]): Unit = {
    val shardCount = Features.get(basename).shardCount
    val bucketCount = loadProperties(basename).getProperty("buckets.count").toInt

    val writers =
      for (s <- 0 until shardCount) yield
        for (b <- 0 until bucketCount) yield
          new FileWriter(Path.toCosts(basename, s, b))

    for ((queryCosts, i) <- costs.zipWithIndex) {
      for ((shardCosts, shardWriters) <- queryCosts zip writers;
           (cost, writer) <- shardCosts zip shardWriters
      ) writer.append(s"$cost\n")
    }

    for (wl <- writers; w <- wl) w.close()
  }

  def main(args: Array[String]): Unit = {

    case class Config(basename: String = null,
                      model: String = null)

    val parser = new OptionParser[Config](CommandName) {

      arg[String]("<basename>")
        .action((x, c) => c.copy(basename = x))
        .text("the prefix of the files")
        .required()

      opt[String]('m', "model")
        .action((x, c) => c.copy(model = x))
        .text("prediction model")
        .required()

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        Spark.session
        val model = RandomForestRegressionModel.load(config.model)
        val costs = costsFromRegressionModel(config.basename, model)
        storeCosts(config.basename, costs)

      case None =>
    }
  }

}
