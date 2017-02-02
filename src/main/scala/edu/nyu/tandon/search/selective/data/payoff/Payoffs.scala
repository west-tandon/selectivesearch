package edu.nyu.tandon.search.selective.data.payoff

import java.io.FileWriter
import java.nio.file.Files

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective._
import edu.nyu.tandon.search.selective.data.Properties
import edu.nyu.tandon.search.selective.data.features.Features
import edu.nyu.tandon.search.selective.data.features.Features._
import edu.nyu.tandon.search.selective.learn.LearnPayoffs.FeaturesColumn
import edu.nyu.tandon.search.selective.learn.PredictPayoffs.PredictedLabelColumn
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql._

import scala.language.implicitConversions

/**
  * @author michal.siedlaczek@nyu.edu
  */
class Payoffs(val payoffs: Iterator[Seq[Seq[Double]]]) extends Iterator[Seq[Seq[Double]]] with LazyLogging {

  override def hasNext: Boolean = payoffs.hasNext
  override def next(): Seq[Seq[Double]] = payoffs.next()

  def store(basename: String): Unit = {
    val properties = Properties.get(basename)
    val shardCount = Features.get(properties).shardCount
    val bucketCount = properties.bucketCount

    val writers =
      for (s <- 0 until shardCount) yield
        for (b <- 0 until bucketCount) yield
          new FileWriter(Path.toPayoffs(basename, s, b))

    for ((queryPayoffs, i) <- payoffs.zipWithIndex) {
      logger.info(s"Processing query $i")
      for ((shardPayoffs, shardWriters) <- queryPayoffs zip writers;
           (payoff, writer) <- shardPayoffs zip shardWriters
      ) writer.append(s"$payoff\n")
    }

    for (wl <- writers; w <- wl) w.close()
  }

}

object Payoffs {

  implicit def doubleSeqSeqIterable2Payoffs(payoffs: Iterator[Seq[Seq[Double]]]): Payoffs = new Payoffs(payoffs)

  def fromPayoffs(basename: String): Payoffs = Load.payoffsAt(basename)

  def fromResults(basename: String): Payoffs = {

    val properties = Properties.get(basename)
    val features = Features.get(properties)

    val baseResults = features.baseResults
    val bucketGlobalResults = Load.bucketizedGlobalResultsAt(basename)

    new Payoffs(
      for ((baseResultsForQuery, bucketizedGlobalResultsForQuery) <- baseResults zip bucketGlobalResults) yield
        for (bucketizedGlobalResultsForShard <- bucketizedGlobalResultsForQuery) yield
          for (bucketizedGlobalResultsForBucket <- bucketizedGlobalResultsForShard) yield
            bucketizedGlobalResultsForBucket.count(baseResultsForQuery.contains(_)).toDouble
    )
  }

  def fromRegressionModel(basename: String, model: RandomForestRegressionModel): Payoffs = {

    val properties = Properties.get(basename)
    val features = Features.get(properties)

    val bucketCount = properties.bucketCount
    val shardCount = features.shardCount
    val queryCount = features.queries.length

    val bucketData = Spark.session.createDataFrame(for (b <- 0 until properties.bucketCount) yield (b, b))
      .withColumnRenamed("_1", BID)
      .drop("_2")
    val raw = features.payoffQueryFeatures
      .join(features.payoffShardFeatures, QID)
      .join(bucketData)

    val featureColumns = properties.queryFeatures("payoff") ++
      properties.shardFeatures("payoff") ++ List(BID)
    val featureAssembler = new VectorAssembler()
      .setInputCols(featureColumns.toArray)
      .setOutputCol(FeaturesColumn)
    val df = featureAssembler.transform(raw)
      .select(QID, SID, BID, FeaturesColumn)

    /* save predictions to temporary file */
    val tmp = Files.createTempDirectory(PredictedLabelColumn)
    model.transform(df).write.mode(Overwrite).save(tmp.toString)
    val predictions = Spark.session.read.parquet(tmp.toString).sort(QID, SID, BID).collect()
    FileUtils.deleteDirectory(tmp.toFile)

    new Payoffs(
      (for (queryId <- 0 until queryCount) yield
        for (shardId <- 0 until shardCount) yield
          for (bucketId <- 0 until bucketCount) yield
            predictions(
              queryId * shardCount * bucketCount +
              shardId * bucketCount +
              bucketId
            ) match {
              case Row(q: Int, s: Int, b: Int, f: Vector, payoff: Double) =>
                require(q == queryId, s"expected query ID $queryId, found $q")
                require(s == shardId, s"expected query ID $shardId, found $s")
                require(b == bucketId, s"expected query ID $bucketId, found $b")
                payoff
            }).iterator
    )

  }

}