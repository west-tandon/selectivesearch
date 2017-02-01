package edu.nyu.tandon.search.selective.learn

import edu.nyu.tandon.search.selective._
import edu.nyu.tandon.search.selective.data.Properties
import edu.nyu.tandon.search.selective.data.features.Features
import edu.nyu.tandon.search.selective.data.features.Features._
import edu.nyu.tandon.utils.Lines
import edu.nyu.tandon.utils.Lines._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode._
import scopt.OptionParser

import scalax.io.StandardOpenOption._

/**
  * @author michal.siedlaczek@nyu.edu
  */
object LearnPayoffs {

  val CommandName = "train-payoffs"

  val FeaturesColumn = "features"
  val LabelColumn = "label"
  val QueryColumn = "query"
  val ShardColumn = "shard"
  val BucketColumn = "bucket"

  def payoffLabels(basename: String, properties: Properties, features: Features): DataFrame =
    (for (shardId <- 0 until features.shardCount) yield
      for (bucketId <- 0 until properties.bucketCount) yield
        Spark.session.createDataFrame(
          Lines.fromFile(s"$basename#$shardId#$bucketId.${properties.payoffLabel}").of[Double].zipWithIndex.map {
            case (value, queryId) => (queryId, shardId, bucketId, value)
          }.toList
        ).withColumnRenamed("_1", QID)
          .withColumnRenamed("_2", SID)
          .withColumnRenamed("_3", BID)
          .withColumnRenamed("_4", properties.payoffLabel)
      ).reduce(_.union(_)).reduce(_.union(_))

  def trainingDataFromBasename(basename: String): DataFrame = {
    val properties = Properties.get(basename)
    val features = Features.get(properties)
    val df = features.queryFeatures
      .join(features.shardFeatures, QID)
      .join(payoffLabels(basename, properties, features), Seq(QID, SID))
    val featureColumns = properties.queryPayoffFeaturesNames ++
      properties.shardPayoffFeaturesNames ++ List(BID)
    val featureAssembler = new VectorAssembler()
      .setInputCols(featureColumns.toArray)
      .setOutputCol(FeaturesColumn)
    featureAssembler.transform(df)
      .withColumnRenamed(properties.payoffLabel, LabelColumn)
      .select(FeaturesColumn, LabelColumn)
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
        model.write.overwrite().save(Path.toPayoffModel(config.basename))
        val testPredictions = model.transform(testData)
        val eval = new RegressionEvaluator().evaluate(testPredictions)
        scalax.file.Path.fromString(Path.toPayoffModelEval(config.basename)).outputStream(WriteTruncate:_*)
          .write(s"$eval")

      case None =>
    }
  }

}
