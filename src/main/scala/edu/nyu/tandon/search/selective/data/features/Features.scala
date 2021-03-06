package edu.nyu.tandon.search.selective.data.features

import java.io.{File, FileInputStream, FileNotFoundException, ObjectInputStream}

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective.data.Properties
import edu.nyu.tandon.search.selective.data.features.Features.{QID, SID}
import edu.nyu.tandon.search.selective.data.results.Result
import edu.nyu.tandon.search.selective.{Spark, Titles2Map}
import edu.nyu.tandon.utils.Lines._
import edu.nyu.tandon.utils.{Lines, ZippedIterator}
import org.apache.spark.sql.DataFrame

import scala.collection.immutable.HashMap

/**
  * @author michal.siedlaczek@nyu.edu
  */
class Features(val basename: String,
               val properties: Properties) extends LazyLogging {

  lazy val titleMap: Map[String, Int] = {
    val map = try {
      val ois = new ObjectInputStream(new FileInputStream(s"$basename.titlemap"))
      val m = ois.readObject().asInstanceOf[HashMap[String, Int]]
      ois.close()
      m
    } catch {
      case e: FileNotFoundException =>
        logger.info("did not find the title map, creating it before proceeding...")
        val m = Titles2Map.titles2map(this)
        logger.info("title map created")
        m
    }
    map.withDefaultValue(-1)
  }

  /* Shards */
  lazy val shardCount: Int = properties.shardCount
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
  def qrelsReference: Seq[Seq[Int]] = {
    val references = Lines.fromFile(s"$basename.qrels").ofSeq.toSeq.groupBy(_.head)
    val sortedQueryIds = references.keys.map(intConverter).toIndexedSeq.sorted
    for (queryId <- sortedQueryIds)
      yield references(s"$queryId")
        .filter(l => intConverter(l(3)) > 0)
        .map(_(2))
        .map(title => {
          val id = titleMap(title)
          if (id == -1) logger.warn(s"title $title is not in the mapping, falling back to default ID=-1")
          id
        })
  }
  def complexFunctionResults: Seq[Seq[Int]] = Lines.fromFile(s"$basename.complex").ofSeq[Int].toList

  /* Documents */
  def documentTitles: Iterator[String] = Lines.fromFile(s"$basename.titles")
  def baseResults: Iterator[Seq[Long]] = Lines.fromFile(s"$basename.results.global").ofSeq[Long]
  def docRanks(shardId: Int): Iterator[Long] = Lines.fromFile(s"$basename#$shardId.docrank").of[Long]
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

  lazy val payoffQueryFeatures: DataFrame = queryFeatures("payoff")
  lazy val payoffShardFeatures: DataFrame = shardFeatures("payoff")
  lazy val costQueryFeatures: DataFrame = queryFeatures("cost")
  lazy val costShardFeatures: DataFrame = shardFeatures("cost")

  def queryFeatures(v: String): DataFrame = (for (feature <- properties.queryFeatures(v)) yield {
    val list = Lines.fromFile(s"$basename.lengths").of[Double].zipWithIndex.map {
      case (value, queryId) => (queryId, value)
    }.toList
    Spark.session.createDataFrame(list)
      .withColumnRenamed("_1", QID)
      .withColumnRenamed("_2", "lengths")
  }).reduce(_.join(_, QID))

  def shardFeatures(v: String): DataFrame = (for (feature <- properties.shardFeatures(v)) yield
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
    if (new File(properties.featuresPath).isAbsolute) new Features(properties.featuresPath, properties)
    else new Features(s"${new File(properties.file).getAbsoluteFile.getParent}/${properties.featuresPath}", properties)
  }
}