package edu.nyu.tandon.search.selective

import edu.nyu.tandon._
import edu.nyu.tandon.search.selective.Path._
import edu.nyu.tandon.utils.ZippableSeq

import scalax.io.Resource

/**
  * @author michal.siedlaczek@nyu.edu
  */
object Load {

  def queryLevelValue[T](basename: String, suffix: String, converter: String => T): Iterable[T] =
    lines(s"$basename$suffix")(converter)

  def queryLevelSequence[T](basename: String, suffix: String, converter: String => T): Iterable[Seq[T]] =
    lines(s"$basename$suffix")(_.split(FieldSplitter).toSeq.map(converter))

  def shardLevelSequence[T](basename: String, suffix: String, converter: String => T): Iterable[Seq[Seq[T]]] = {
    val shardCount = loadProperties(basename).getProperty("shards.count").toInt
    new ZippableSeq(
      for (s <- 0 until shardCount) yield
        Resource.fromFile(s"$basename#$s$suffix").lines()
          .map(_.split(FieldSplitter).toSeq.map(converter)).toIterable
    ).zipped
  }

  def shardLevelValue[T](basename: String, suffix: String, converter: String => T): Iterable[Seq[T]] = {
    val shardCount = loadProperties(basename).getProperty("shards.count").toInt
    new ZippableSeq(
      for (s <- 0 until shardCount) yield
        Resource.fromFile(s"$basename#$s$suffix").lines().map(converter).toIterable
    ).zipped
  }

  def bucketLevelValue[T](basename: String, suffix: String, converter: String => T): Iterable[Seq[Seq[T]]] = {
    val shardCount = loadProperties(basename).getProperty("shards.count").toInt
    val bucketCount = loadProperties(basename).getProperty("buckets.count").toInt
    new ZippableSeq(
      for (s <- 0 until shardCount) yield
        new ZippableSeq(
          for (b <- 0 until bucketCount) yield
            Resource.fromFile(s"$basename#$s#$b$suffix").lines()
              .map(converter).toIterable
        ).zipped
    ).zipped
  }

  /* Query level */
  def queryLengthsAt(basename: String): Iterable[Int] = queryLevelValue(basename, QueryLengthsSuffix, _.toInt)
  def queriesAt(basename: String): Iterable[String] = queryLevelValue(basename, QueriesSuffix, _.toString)
  def titlesAt(basename: String): Iterable[String] = queryLevelValue(basename, TitlesSuffix, _.toString)
  def trecIdsAt(basename: String): Iterable[Long] = queryLevelValue(basename, TrecIdSuffix, _.toLong)

  def globalResultDocumentsAt(basename: String): Iterable[Seq[Long]] = queryLevelValue(basename, s"$ResultsSuffix$GlobalSuffix", lineToLongs(_).sorted)
  def selectedDocumentsAt(basename: String): Iterable[Seq[Long]] = queryLevelSequence(basename, s"$SelectedSuffix$DocumentsSuffix", _.toLong)
  def selectedScoresAt(basename: String): Iterable[Seq[Double]] = queryLevelSequence(basename, s"$SelectedSuffix$ScoresSuffix", _.toDouble)

  /* Shard level */
  def reddeScoresAt(basename: String): Iterable[Seq[Double]] = shardLevelValue(basename, ReDDESuffix, _.toDouble)
  def shrkcScoresAt(basename: String): Iterable[Seq[Double]] = shardLevelValue(basename, ShRkCSuffix, _.toDouble)
  def shardGlobalResultsAt(basename: String): Iterable[Seq[Double]] = shardLevelValue(basename, s"$ResultsSuffix$GlobalSuffix", _.toDouble)

  /* Bucket level */
  def payoffsAt(basename: String): Iterable[Seq[Seq[Double]]] = bucketLevelValue(basename, PayoffSuffix, _.toDouble)

}
