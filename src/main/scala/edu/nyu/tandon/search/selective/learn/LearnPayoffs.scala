package edu.nyu.tandon.search.selective.learn

import edu.nyu.tandon.search.selective._
import edu.nyu.tandon.search.selective.data.features.Features
import edu.nyu.tandon.search.selective.data.payoff.Payoffs
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode._
import scopt.OptionParser

import scalax.io.Resource

/**
  * @author michal.siedlaczek@nyu.edu
  */
object LearnPayoffs {

  val CommandName = "learn-payoffs"

  val FeaturesColumn = "features"
  val LabelColumn = "label"
  val QueryColumn = "query"
  val ShardColumn = "shard"
  val BucketColumn = "bucket"

  def trainingDataFromBasename(basename: String): DataFrame = {
    val features = Features.get(basename)
    val lengths = features.queryLengths
    val redde = features.reddeScores
    val shrkc = features.shrkcScores
    val labels = Payoffs.fromPayoffs(basename)
    val data = for ((((queryLength, reddeScores), shrkcScores), payoffs) <- lengths.zip(redde).zip(shrkc).zip(labels);
         ((shardReddeScore, shardShrkcScore), shardPayoffs) <- reddeScores.zip(shrkcScores).zip(payoffs);
         (payoff, bucket) <- shardPayoffs.zipWithIndex)
      yield (Vectors.dense(queryLength, shardReddeScore, shardShrkcScore, bucket.toDouble), payoff)
    Spark.session
      .createDataFrame(data.toSeq)
      .withColumnRenamed("_1", FeaturesColumn)
      .withColumnRenamed("_2", LabelColumn)
  }

  def main(args: Array[String]): Unit = {

    case class Config(basename: String = null)

    val parser = new OptionParser[Config](CommandName) {

      opt[String]('n', "basename")
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
        model.write.overwrite().save(Path.toModel(config.basename))
        val testPredictions = model.transform(testData)
        val eval = new RegressionEvaluator().evaluate(testPredictions)
        Resource.fromFile(Path.toModelEval(config.basename)).write(s"$eval")

      case None =>
    }
  }

}
