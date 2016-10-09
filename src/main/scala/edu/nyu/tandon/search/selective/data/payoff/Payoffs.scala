package edu.nyu.tandon.search.selective.data.payoff

import java.io.FileWriter

import edu.nyu.tandon._
import edu.nyu.tandon.search.selective._
import edu.nyu.tandon.search.selective.learn.LearnPayoffs
import edu.nyu.tandon.utils.ZippableSeq
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.sql.SparkSession

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

    new Payoffs(
      // For each query
      for (((queryLength, reddeScores), shrkcScores) <- lengths.zip(redde).zip(shrkc)) yield
        // For each shard
        for ((shardReddeScore, shardShrkcScore) <- reddeScores zip shrkcScores) yield
          // For each bucket
          for (bucket <- 0 until bucketCount) yield {
            val df = SparkSession.builder()
              .master("local[*]")
              .appName(Payoffs.getClass.getName)
              .getOrCreate()
              .createDataFrame(Seq((Vectors.dense(queryLength, shardReddeScore, shardShrkcScore, bucket.toDouble), 0.0)))
              .withColumnRenamed("_1", LearnPayoffs.FeaturesColumn)
            val prediction = model.transform(df)
            prediction.toLocalIterator().next().getAs[Double](LearnPayoffs.LabelColumn)
        }
    )

  }

}