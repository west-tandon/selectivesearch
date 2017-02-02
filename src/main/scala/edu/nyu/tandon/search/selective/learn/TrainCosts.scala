package edu.nyu.tandon.search.selective.learn

import edu.nyu.tandon.search.selective.data.features.Features
import edu.nyu.tandon.search.selective.data.features.Features._
import edu.nyu.tandon.search.selective.{Path, Spark}
import edu.nyu.tandon.utils.Lines
import edu.nyu.tandon.utils.Lines._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql._
import scopt.OptionParser

import scala.language.implicitConversions
import scalax.io.StandardOpenOption._

/**
  * @author michal.siedlaczek@nyu.edu
  */
object TrainCosts {

  val CommandName = "train-costs"

  val FeaturesColumn = "features"
  val LabelColumn = "label"

  def costLabels(features: Features): DataFrame =
    (for (shardId <- 0 until features.shardCount) yield
        Spark.session.createDataFrame(
          Lines.fromFile(s"${features.basename}#$shardId.${features.properties.costLabel}").of[Double].zipWithIndex.map {
            case (value, queryId) => (queryId, shardId, value)
          }.toList
        ).withColumnRenamed("_1", QID)
          .withColumnRenamed("_2", SID)
          .withColumnRenamed("_3", features.properties.costLabel)
      ).reduce(_.union(_))

  def trainingDataFromBasename(basename: String): DataFrame = {

    val features = Features.get(basename)

    val queryFeatures = features.costQueryFeatures
    val shardFeatures = features.costShardFeatures
    val costs = costLabels(features)

    val df = queryFeatures
      .join(shardFeatures, QID)
      .join(costs, Seq(QID, SID))

    val featureColumns = features.properties.queryFeatures("cost") ++
      features.properties.shardFeatures("cost")

    val featureAssembler = new VectorAssembler()
      .setInputCols(featureColumns.toArray)
      .setOutputCol(FeaturesColumn)
    featureAssembler.transform(df)
      .withColumnRenamed(features.properties.costLabel, LabelColumn)
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
        model.write.overwrite().save(Path.toCostModel(config.basename))
        val testPredictions = model.transform(testData)
        val eval = new RegressionEvaluator().evaluate(testPredictions)
        scalax.file.Path.fromString(Path.toCostModelEval(config.basename)).outputStream(WriteTruncate:_*)
          .write(s"$eval")

      case None =>
    }
  }

}
