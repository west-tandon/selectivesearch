package edu.nyu.tandon.search.selective

import edu.nyu.tandon._
import edu.nyu.tandon.search.selective.Path._
import edu.nyu.tandon.utils.BulkIterator

/**
  * @author michal.siedlaczek@nyu.edu
  */
object Load {

  def queryLevelValue[T](basename: String, suffix: String, converter: String => T): Iterator[T] =
    lines(s"$basename$suffix")(converter)

  def queryLevelSequence[T](basename: String, suffix: String, converter: String => T): Iterator[Seq[T]] =
    lines(s"$basename$suffix")(_.split(FieldSplitter).filter(_.length > 0).toSeq.map(converter))

  def shardLevelSequence[T](basename: String, suffix: String, converter: String => T): Iterator[Seq[Seq[T]]] = {
    val shardCount = loadProperties(basename).getProperty("shards.count").toInt
    new BulkIterator(
      for (s <- 0 until shardCount) yield
        lines(s"$basename#$s$suffix")(_.split(FieldSplitter).filter(_.length > 0).toSeq.map(converter))
    )
  }

  def shardLevelValue[T](basename: String, suffix: String, converter: String => T): Iterator[Seq[T]] = {
    val shardCount = loadProperties(basename).getProperty("shards.count").toInt
    new BulkIterator(
      for (s <- 0 until shardCount) yield lines(s"$basename#$s$suffix")(converter)
    )
  }

  def bucketLevelValue[T](basename: String, suffix: String, converter: String => T): Iterator[Seq[Seq[T]]] = {
    val shardCount = loadProperties(basename).getProperty("shards.count").toInt
    val bucketCount = loadProperties(basename).getProperty("buckets.count").toInt
    new BulkIterator(
      for (s <- 0 until shardCount) yield
        new BulkIterator(
          for (b <- 0 until bucketCount) yield lines(s"$basename#$s#$b$suffix")(converter)
        )
    )
  }

  def bucketLevelSequence[T](basename: String, suffix: String, converter: String => T): Iterator[Seq[Seq[Seq[T]]]] = {
    val shardCount = loadProperties(basename).getProperty("shards.count").toInt
    val bucketCount = loadProperties(basename).getProperty("buckets.count").toInt
    new BulkIterator(
      for (s <- 0 until shardCount) yield
        new BulkIterator(
          for (b <- 0 until bucketCount)
            yield lines(s"$basename#$s#$b$suffix")(_.split(FieldSplitter).filter(_.length > 0).toSeq.map(converter))
        )
    )
  }

  /* Query level */
  def queryLengthsAt(basename: String): Iterator[Int] = queryLevelValue(basename, QueryLengthsSuffix, _.toInt)
  def queriesAt(basename: String): Iterator[String] = queryLevelValue(basename, QueriesSuffix, _.toString)
  def titlesAt(basename: String): Iterator[String] = queryLevelValue(basename, TitlesSuffix, _.toString)
  def trecIdsAt(basename: String): Iterator[Long] = queryLevelValue(basename, TrecIdSuffix, _.toLong)

  def globalResultDocumentsAt(basename: String): Iterator[Seq[Long]] = queryLevelValue(base(basename), s"$ResultsSuffix$GlobalSuffix", lineToLongs(_).sorted)
  def selectedDocumentsAt(basename: String): Iterator[Seq[Long]] = queryLevelSequence(basename, s"$SelectedSuffix$DocumentsSuffix", _.toLong)
  def selectedScoresAt(basename: String): Iterator[Seq[Double]] = queryLevelSequence(basename, s"$SelectedSuffix$ScoresSuffix", _.toDouble)

  /* Shard level */
  def reddeScoresAt(basename: String): Iterator[Seq[Double]] = shardLevelValue(basename, ReDDESuffix, _.toDouble)
  def shrkcScoresAt(basename: String): Iterator[Seq[Double]] = shardLevelValue(basename, ShRkCSuffix, _.toDouble)
  def shardGlobalResultsAt(basename: String): Iterator[Seq[Seq[Double]]] = shardLevelSequence(basename, s"$ResultsSuffix$GlobalSuffix", _.toDouble)

  /* Bucket level */
  def payoffsAt(basename: String): Iterator[Seq[Seq[Double]]] = bucketLevelValue(basename, PayoffSuffix, _.toDouble)
  def bucketizedGlobalResultsAt(basename: String): Iterator[Seq[Seq[Seq[Long]]]] = bucketLevelSequence(basename, s"$ResultsSuffix$GlobalSuffix", _.toLong)

}
