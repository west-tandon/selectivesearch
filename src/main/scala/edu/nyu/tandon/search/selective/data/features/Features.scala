package edu.nyu.tandon.search.selective.data.features

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective.Spark
import edu.nyu.tandon.search.selective.data.Properties
import edu.nyu.tandon.search.selective.data.features.Features.{BID, QID, SID}
import edu.nyu.tandon.search.selective.data.results.Result
import edu.nyu.tandon.utils.{Lines, ZippedIterator}
import edu.nyu.tandon.utils.Lines._
import org.apache.spark.sql.DataFrame

/**
  * @author michal.siedlaczek@nyu.edu
  */
class Features(val basename: String,
               val queryFeatureNames: List[String],
               val shardFeatureNames: List[String],
               val bucketFeatureNames: List[String],
               val bucketCount: Int) extends LazyLogging {

  /* Shards */
  lazy val shardCount: Int = shardSizes.length
  lazy val shardSizes: IndexedSeq[Long] = Lines.fromFile(s"$basename.sizes").of[Long].toIndexedSeq
  lazy val avgTime: Double = Lines.fromFile(s"$basename.time.avg").of[Double].next()
  def shardSize(id: Int): Long = shardSizes(id)

  def reddeScores: Iterator[Seq[Double]] =
    ZippedIterator(for (s <- 0 until shardCount) yield Lines.fromFile(s"$basename#$s.redde").of[Double]).strict
  def shrkcScores: Iterator[Seq[Double]] =
    ZippedIterator(for (s <- 0 until shardCount) yield Lines.fromFile(s"$basename#$s.shrkc").of[Double]).strict
  def tailyScores: Iterator[Seq[Double]] =
    ZippedIterator(for (s <- 0 until shardCount) yield Lines.fromFile(s"$basename#$s.taily").of[Double]).strict

  def costs(shardId: Int): Iterator[Double] = Lines.fromFile(s"$basename#$shardId.cost").of[Double]
  def costs: Iterator[Seq[Double]] =
    ZippedIterator(for (s <- 0 until shardCount) yield Lines.fromFile(s"$basename#$s.cost").of[Double]).strict

  def times(shardId: Int): Iterator[Double] = Lines.fromFile(s"$basename#$shardId.time").of[Double]
  def times: Iterator[Seq[Double]] =
    ZippedIterator(for (s <- 0 until shardCount) yield Lines.fromFile(s"$basename#$s.time").of[Double]).strict

  def maxListLen1: Iterator[Seq[Double]] =
    ZippedIterator(for (s <- 0 until shardCount) yield Lines.fromFile(s"$basename#$s.maxlist1").of[Double]).strict
  def maxListLen2: Iterator[Seq[Double]] =
    ZippedIterator(for (s <- 0 until shardCount) yield Lines.fromFile(s"$basename#$s.maxlist2").of[Double]).strict
  def minListLen1: Iterator[Seq[Double]] =
    ZippedIterator(for (s <- 0 until shardCount) yield Lines.fromFile(s"$basename#$s.minlist1").of[Double]).strict
  def minListLen2: Iterator[Seq[Double]] =
    ZippedIterator(for (s <- 0 until shardCount) yield Lines.fromFile(s"$basename#$s.minlist2").of[Double]).strict
  def sumListLen: Iterator[Seq[Double]] =
    ZippedIterator(for (s <- 0 until shardCount) yield Lines.fromFile(s"$basename#$s.sumlist").of[Double]).strict

  /* Queries */
  def queries: Iterator[String] = Lines.fromFile(s"$basename.queries")
  def queryLengths: Iterator[Int] = Lines.fromFile(s"$basename.lengths").of[Int]
  def trecIds: Iterator[Long] = Lines.fromFile(s"$basename.trecid").of[Long]

  /* Documents */
  def documentTitles: Iterator[String] = Lines.fromFile(s"$basename.titles")
  def baseResults: Iterator[Seq[Long]] = Lines.fromFile(s"$basename.results.global").ofSeq[Long]
  def shardResults: Iterator[IndexedSeq[Seq[Result]]] =
    ZippedIterator(
      for (s <- 0 until shardCount) yield {
        val localIds = Lines.fromFile(s"$basename#$s.results.local").ofSeq[Long]
        val globalIds = Lines.fromFile(s"$basename#$s.results.global").ofSeq[Long]
        val scores = Lines.fromFile(s"$basename#$s.results.scores").ofSeq[Double]
        for (((l, g), s) <- localIds.zip(globalIds).zip(scores)) yield {
          require(l.length == g.length && g.length == s.length,
            s"different number of elements in a line among (local, global and scores) = (${l.length}, ${g.length}, ${s.length})")
          for (((localId, globalId), score) <- l.zip(g).zip(s)) yield Result(localId, globalId, score)
        }
      }
    ).strict.map(_.toIndexedSeq)
  def shardResults(shardId: Int): Iterator[Seq[Result]] = {
    val localIds = Lines.fromFile(s"$basename#$shardId.results.local").ofSeq[Long]
    val globalIds = Lines.fromFile(s"$basename#$shardId.results.global").ofSeq[Long]
    val scores = Lines.fromFile(s"$basename#$shardId.results.scores").ofSeq[Double]
    for (((l, g), s) <- localIds.zip(globalIds).zip(scores)) yield {
      require(l.length == g.length && g.length == s.length,
        s"different number of elements in a line among (local, global and scores) = (${l.length}, ${g.length}, ${s.length})")
      for (((localId, globalId), score) <- l.zip(g).zip(s)) yield Result(localId, globalId, score)
    }
  }

  lazy val queryFeatures: DataFrame = {
    //(for (feature <- queryFeatureNames) yield
    logger.debug("Loading lines")
    val list = Lines.fromFile(s"$basename.lengths").of[Double].zipWithIndex.map {
      case (value, queryId) => (queryId, value)
    }.toList
    logger.debug("Creating data frame from lines")
    Spark.session.createDataFrame(list)
      .withColumnRenamed("_1", QID)
      .withColumnRenamed("_2", "lengths")
  }
//    ).reduce(_.join(_, QID))

  lazy val shardFeatures: DataFrame = (for (feature <- shardFeatureNames) yield
    (for (shardId <- 0 until shardCount) yield
      Spark.session.createDataFrame(
        Lines.fromFile(s"$basename#$shardId.$feature").of[Double].zipWithIndex.map {
          case (value, queryId) => (queryId, shardId, value)
        }.toList
      ).withColumnRenamed("_1", QID)
        .withColumnRenamed("_2", SID)
        .withColumnRenamed("_3", feature)).reduce(_.union(_))
    ).reduce(_.join(_, Seq(QID, SID)))

}

object Features {

  val QID = "qid"
  val SID = "sid"
  val BID = "bid"

  def get(basename: String): Features = {
    get(Properties.get(basename))
  }
  def get(properties: Properties): Features = {
    if (new File(properties.featuresPath).isAbsolute) new Features(properties.featuresPath,
      properties.queryPayoffFeaturesNames,
      properties.shardPayoffFeaturesNames,
      properties.bucketPayoffFeaturesNames,
      properties.bucketCount)
    else new Features(s"${new File(properties.file).getAbsoluteFile.getParent}/${properties.featuresPath}",
      properties.queryPayoffFeaturesNames,
      properties.shardPayoffFeaturesNames,
      properties.bucketPayoffFeaturesNames,
      properties.bucketCount)
  }
}