package edu.nyu.tandon.search.selective

import edu.nyu.tandon._
import edu.nyu.tandon.search.selective.Path._
import edu.nyu.tandon.search.selective.data.features.Features
import edu.nyu.tandon.utils.ZippedIterator

/**
  * @author michal.siedlaczek@nyu.edu
  */
object Load {

  def queryLevelValue[T](basename: String, suffix: String, converter: String => T): Iterator[T] =
    lines(s"$basename$suffix")(converter)

  def queryLevelSequence[T](basename: String, suffix: String, converter: String => T): Iterator[Seq[T]] =
    lines(s"$basename$suffix")(_.split(FieldSplitter).filter(_.length > 0).toSeq.map(converter))

  def shardLevelSequence[T](basename: String, suffix: String, converter: String => T): Iterator[Seq[Seq[T]]] = {
    val shardCount = Features.get(basename).shardCount
    new ZippedIterator(
      for (s <- 0 until shardCount) yield
        lines(s"$basename#$s$suffix")(_.split(FieldSplitter).filter(_.length > 0).toSeq.map(converter))
    )
  }

  def shardLevelValue[T](basename: String, suffix: String, converter: String => T): Iterator[Seq[T]] = {
    val shardCount = Features.get(basename).shardCount
    new ZippedIterator(
      for (s <- 0 until shardCount) yield lines(s"$basename#$s$suffix")(converter)
    )
  }

  def bucketLevelValue[T](basename: String, suffix: String, converter: String => T): Iterator[Seq[Seq[T]]] = {
    val shardCount = Features.get(basename).shardCount
    val bucketCount = loadProperties(basename).getProperty("buckets.count").toInt
    new ZippedIterator(
      for (s <- 0 until shardCount) yield
        new ZippedIterator(
          for (b <- 0 until bucketCount) yield lines(s"$basename#$s#$b$suffix")(converter)
        )
    )
  }

  def bucketLevelSequence[T](basename: String, suffix: String, converter: String => T): Iterator[Seq[Seq[Seq[T]]]] = {
    val shardCount = Features.get(basename).shardCount
    val bucketCount = loadProperties(basename).getProperty("buckets.count").toInt
    new ZippedIterator(
      for (s <- 0 until shardCount) yield
        new ZippedIterator(
          for (b <- 0 until bucketCount)
            yield lines(s"$basename#$s#$b$suffix")(_.split(FieldSplitter).filter(_.length > 0).toSeq.map(converter))
        )
    )
  }

  def selectedDocumentsAt(basename: String): Iterator[Seq[Long]] = queryLevelSequence(basename, s"$SelectedSuffix$DocumentsSuffix", _.toLong)
  def selectedScoresAt(basename: String): Iterator[Seq[Double]] = queryLevelSequence(basename, s"$SelectedSuffix$ScoresSuffix", _.toDouble)

  def payoffsAt(basename: String): Iterator[Seq[Seq[Double]]] = bucketLevelValue(basename, PayoffSuffix, _.toDouble)
  def bucketizedGlobalResultsAt(basename: String): Iterator[Seq[Seq[Seq[Long]]]] = bucketLevelSequence(basename, s"$ResultsSuffix$GlobalSuffix", _.toLong)

}
