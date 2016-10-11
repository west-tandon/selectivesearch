package edu.nyu.tandon.search.selective.data.payoff

import java.io.FileWriter
import java.nio.file.Files

import edu.nyu.tandon._
import edu.nyu.tandon.search.selective._
import edu.nyu.tandon.search.selective.learn.LearnPayoffs.{BucketColumn, FeaturesColumn, QueryColumn, ShardColumn}
import edu.nyu.tandon.search.selective.learn.PredictPayoffs.PredictedLabelColumn
import edu.nyu.tandon.utils.ZippableSeq
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode.Overwrite

import scala.language.implicitConversions

/**
  * @author michal.siedlaczek@nyu.edu
  */
class Payoffs(val payoffs: Iterable[Seq[Seq[Double]]]) extends Iterable[Seq[Seq[Double]]] {

  override def iterator: Iterator[Seq[Seq[Double]]] = payoffs.iterator

  def store(basename: String): Unit = {
    val shardCount = loadProperties(basename).getProperty("shards.count").toInt
    val bucketCount = loadProperties(basename).getProperty("buckets.count").toInt

    val writers =
      for (s <- 0 until shardCount) yield
        for (b <- 0 until bucketCount) yield
          new FileWriter(Path.toPayoffs(basename, s, b))

    for (queryPayoffs <- payoffs;
        (shardPayoffs, shardWriters) <- queryPayoffs zip writers;
        (payoff, writer) <- shardPayoffs zip shardWriters
    ) writer.append(s"$payoff\n")

    for (wl <- writers; w <- wl) w.close()
  }

}

object Payoffs {

  implicit def doubleSeqSeqIterable2Payoffs(payoffs: Iterable[Seq[Seq[Double]]]): Payoffs = new Payoffs(payoffs)

  def fromPayoffs(basename: String): Payoffs = Load.payoffsAt(basename)

  def fromResults(basename: String): Payoffs = {
    val shardCount = loadProperties(basename).getProperty("shards.count").toInt
    val bucketCount = loadProperties(basename).getProperty("buckets.count").toInt
    val globalResults = Load.globalResultDocumentsAt(basename)
    new Payoffs(
      new ZippableSeq(for (s <- 0 until shardCount) yield
        new ZippableSeq(for (b <- 0 until bucketCount) yield {
          val shardResultsSorted = lines(Path.toGlobalResults(basename, s, b)).map(lineToLongs(_).sorted).toIterable
          val filteredShardResults = globalResults.zip(shardResultsSorted) map {
            case (global, shard) => shard.count(global.contains(_)).toDouble
          }
          filteredShardResults
        }).zipped
      ).zipped
    )
  }

  def fromRegressionModel(basename: String, model: RandomForestRegressionModel): Payoffs = {
    val shardCount = loadProperties(basename).getProperty("shards.count").toInt
    val bucketCount = loadProperties(basename).getProperty("buckets.count").toInt
    val queryCount = Load.queriesAt(basename).size

    val lengths = Load.queryLengthsAt(basename)
    val redde = Load.reddeScoresAt(basename)
    val shrkc = Load.shrkcScoresAt(basename)

    val data = for (((queryLength, queryId), (reddeScores, shrkcScores)) <- lengths.zipWithIndex zip (redde zip shrkc);
                    ((shardReddeScore, shardShrkcScore), shardId) <- reddeScores.zip(shrkcScores).zipWithIndex;
                    bucket <- 0 until bucketCount) yield
      (queryId, shardId, bucket, Vectors.dense(queryLength, shardReddeScore, shardShrkcScore, bucket.toDouble))

    val df = Spark.session.createDataFrame(data.toSeq)
      .withColumnRenamed("_1", QueryColumn)
      .withColumnRenamed("_2", ShardColumn)
      .withColumnRenamed("_3", BucketColumn)
      .withColumnRenamed("_4", FeaturesColumn)

    /* save predictions to temporary file */
    val tmp = Files.createTempDirectory(PredictedLabelColumn)
    model.transform(df).write.mode(Overwrite).save(tmp.toString)
    val predictions = Spark.session.read.parquet(tmp.toString).sort(QueryColumn, ShardColumn, BucketColumn).collect()
    FileUtils.deleteDirectory(tmp.toFile)

    new Payoffs(
      for (queryId <- 0 until queryCount) yield
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
            }
    )

  }

}