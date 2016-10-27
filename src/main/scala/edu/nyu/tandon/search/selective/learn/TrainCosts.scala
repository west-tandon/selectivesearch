package edu.nyu.tandon.search.selective.learn

import edu.nyu.tandon.search.selective.data.features.Features
import edu.nyu.tandon.search.selective.{Path, Spark}
import edu.nyu.tandon.utils.TupleIterators._
import edu.nyu.tandon.utils.TupleIterables._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql._
import scopt.OptionParser

import scala.language.implicitConversions
import scalax.io.Resource

/**
  * @author michal.siedlaczek@nyu.edu
  */
object TrainCosts {

  val CommandName = "train-payoffs"

  val FeaturesColumn = "features"
  val LabelColumn = "label"
  val ShardColumn = "shard"
  val QueryColumn = "query"

  def trainingDataFromBasename(basename: String): DataFrame = {

    val features = Features.get(basename)
    val featureIterator = features.queryLengths
      .zip(features.maxListLen1)
      .flatZip(features.maxListLen2)
      .flatZip(features.minListLen1)
      .flatZip(features.minListLen2)
      .flatZip(features.sumListLen)
      .flatZip(features.costs)

    val data = for ((queryLength, allMaxLL1, allMaxLL2, allMinLL1, allMinLL2, allSumLen, allCosts) <- featureIterator) yield {
      val shardFeatureIterator = allMaxLL1
        .zip(allMaxLL2)
        .flatZip(allMinLL1)
        .flatZip(allMinLL2)
        .flatZip(allSumLen)
        .flatZip(allCosts)
      for ((maxLL1, maxLL2, minLL1, minLL2, sumLen, cost) <- shardFeatureIterator)
        yield (Vectors.dense(queryLength, maxLL1, maxLL2, minLL1, minLL2, sumLen), cost)
    }

    Spark.session
      .createDataFrame(data.flatten.toSeq)
      .withColumnRenamed("_1", FeaturesColumn)
      .withColumnRenamed("_2", LabelColumn)
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

        trainingDataFromBasename(config.basename).write.mode(Overwrite).save(s"${config.basename}.data")

        val Array(trainingData, testData) = Spark.session
          .read.parquet(s"${config.basename}.data").randomSplit(Array(0.7, 0.3))

        val regressor = new RandomForestRegressor()
        val model = regressor.fit(trainingData)
        model.write.overwrite().save(Path.toCostModel(config.basename))
        val testPredictions = model.transform(testData)
        val eval = new RegressionEvaluator().evaluate(testPredictions)
        Resource.fromFile(Path.toCostModelEval(config.basename)).write(s"$eval")

      case None =>
    }
  }

}
