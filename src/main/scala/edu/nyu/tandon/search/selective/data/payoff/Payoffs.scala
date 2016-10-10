package edu.nyu.tandon.search.selective.data.payoff

import java.io.FileWriter
import java.nio.file.{Files, Paths}

import edu.nyu.tandon._
import edu.nyu.tandon.search.selective._
import edu.nyu.tandon.search.selective.learn.LearnPayoffs.{BucketColumn, FeaturesColumn, QueryColumn, ShardColumn}
import edu.nyu.tandon.search.selective.learn.PredictPayoffs.PredictedLabelColumn
import edu.nyu.tandon.utils.ZippableSeq
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.{Row, SaveMode}

import scala.language.implicitConversions
import scalax.io.Resource

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
          new FileWriter(s"$basename#$s#$b$PayoffSuffix")

    for (queryPayoffs <- payoffs;
        (shardPayoffs, shardWriters) <- queryPayoffs zip writers;
        (payoff, writer) <- shardPayoffs zip shardWriters
    ) writer.append(s"$payoff\n")

    for (wl <- writers; w <- wl) w.close()
  }

}

object Payoffs {

  implicit def doubleSeqSeqIterable2Payoffs(payoffs: Iterable[Seq[Seq[Double]]]): Payoffs = new Payoffs(payoffs)

  def fromPayoffs(basename: String): Payoffs = bucketLevelValue(basename, PayoffSuffix, _.toDouble)

  def fromResults(basename: String): Payoffs = {
    val shardCount = loadProperties(basename).getProperty("shards.count").toInt
    val bucketCount = loadProperties(basename).getProperty("buckets.count").toInt
    val globalResults = lines(s"$basename$ResultsSuffix$GlobalSuffix").map(lineToLongs(_).sorted)
    new Payoffs(
      new ZippableSeq(for (s <- 0 until shardCount) yield
        new ZippableSeq(for (b <- 0 until bucketCount) yield {
          val shardResultsSorted = lines(s"$basename#$s#$b$ResultsSuffix$GlobalSuffix").map(lineToLongs(_).sorted)
          val filteredShardResults = globalResults.zip(shardResultsSorted) map {
            case (global, shard) => shard.count(global.contains(_)).toDouble
          }
          filteredShardResults.toIterable
        }).zipped
      ).zipped
    )
  }

  def fromRegressionModel(basename: String, model: RandomForestRegressionModel): Payoffs = {
    val bucketCount = loadProperties(basename).getProperty("buckets.count").toInt

    val lengths = Resource.fromFile(s"$basename$QueryLengthsSuffix").lines().map(_.toDouble).toIterable
    val redde = shardLevelValue(basename, ReDDESuffix, _.toDouble)
    val shrkc = shardLevelValue(basename, ShRkCSuffix, _.toDouble)

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
    val predictions = Spark.session.read.parquet(tmp.toString)

    val payoffs =
      (for (Row(queryId: Int) <- predictions.select(QueryColumn).distinct().collect()) yield {
        val queryData = predictions.filter(predictions(QueryColumn) === queryId)
        (for (Row(shardId: Int) <- queryData.select(ShardColumn).distinct().collect()) yield {
          val shardData = queryData.filter(queryData(ShardColumn) === shardId)
          (for (Row(bucketId: Int) <- shardData.select(BucketColumn).distinct().collect()) yield {
            val bucketData = shardData
              .filter(shardData(BucketColumn) === bucketId)
              .select(PredictedLabelColumn)
              .collect()
            require(bucketData.length == 1,
              s"there should be one row for (query, shard, bucket) but there is ${bucketData.length}")
            bucketData.head match {
              case Row(payoff: Double) => payoff
            }
          }).toSeq
        }).toSeq
      }).toIterable

    FileUtils.deleteDirectory(tmp.toFile)
    new Payoffs(payoffs)

  }

}