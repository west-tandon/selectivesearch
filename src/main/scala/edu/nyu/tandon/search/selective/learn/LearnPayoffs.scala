package edu.nyu.tandon.search.selective.learn

import edu.nyu.tandon.search.selective._
import edu.nyu.tandon.search.selective.data.payoff.Payoffs
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser

import scalax.io.Resource

/**
  * @author michal.siedlaczek@nyu.edu
  */
object LearnPayoffs {

  val FeaturesColumn = "features"
  val LabelColumn = "label"

  def trainingDataFromBasename(basename: String): DataFrame = {
    val lengths = Resource.fromFile(s"$basename$QueryLengthsSuffix").lines().map(_.toDouble).toIterable
    val redde = shardLevelValue(basename, ReDDESuffix, _.toDouble)
    val shrkc = shardLevelValue(basename, ShRkCSuffix, _.toDouble)
    val labels = Payoffs.fromPayoffs(basename)
    val data = for ((((queryLength, reddeScores), shrkcScores), payoffs) <- lengths.zip(redde).zip(shrkc).zip(labels);
         ((shardReddeScore, shardShrkcScore), shardPayoffs) <- reddeScores.zip(shrkcScores).zip(payoffs);
         (payoff, bucket) <- shardPayoffs.zipWithIndex)
      yield (Vectors.dense(queryLength, shardReddeScore, shardShrkcScore, bucket.toDouble), payoff)
    SparkSession.builder()
      .master("local[*]")
      .appName(LearnPayoffs.getClass.getName)
      .getOrCreate()
      .createDataFrame(data.toSeq)
      .withColumnRenamed("_1", FeaturesColumn)
      .withColumnRenamed("_2", LabelColumn)
  }

  def main(args: Array[String]): Unit = {

    case class Config(basename: String = null)

    val parser = new OptionParser[Config](this.getClass.getSimpleName) {

      opt[String]('n', "basename")
        .action((x, c) => c.copy(basename = x))
        .text("the prefix of the files")
        .required()

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        val Array(trainingData, testData) = trainingDataFromBasename(config.basename).randomSplit(Array(0.7, 0.3))
        val regressor = new RandomForestRegressor()
        val model = regressor.fit(trainingData)
        model.write.overwrite().save(s"${config.basename}$ModelSuffix")
        val testPredictions = model.transform(testData)
        val eval = new RegressionEvaluator().evaluate(testPredictions)
        Resource.fromFile(s"${config.basename}$ModelSuffix$EvalSuffix").write(s"$eval")

      case None =>
    }
  }

}
