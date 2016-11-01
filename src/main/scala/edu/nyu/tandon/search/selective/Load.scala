package edu.nyu.tandon.search.selective

import edu.nyu.tandon.search.selective.Path._
import edu.nyu.tandon.search.selective.data.Properties
import edu.nyu.tandon.search.selective.data.features.Features
import edu.nyu.tandon.utils.{Lines, ZippedIterator}
import edu.nyu.tandon.utils.Lines._

/**
  * @author michal.siedlaczek@nyu.edu
  */
object Load {

//  def queryLevelValue[T](basename: String, suffix: String, converter: String => T): Iterator[T] =
//    lines(s"$basename$suffix")(converter)

  def queryLevelSequence[T](basename: String, suffix: String)(implicit converter: String => T): Iterator[Seq[T]] =
    Lines.fromFile(s"$basename$suffix").ofSeq(converter)

  def shardLevelSequence[T](basename: String, suffix: String)(implicit converter: String => T): Iterator[Seq[Seq[T]]] = {
    val shardCount = Features.get(basename).shardCount
    new ZippedIterator(
      for (s <- 0 until shardCount) yield Lines.fromFile(s"$basename#$s$suffix").ofSeq(converter)
    )
  }

//  def shardLevelValue[T](basename: String, suffix: String, converter: String => T): Iterator[Seq[T]] = {
//    val shardCount = Features.get(basename).shardCount
//    new ZippedIterator(
//      for (s <- 0 until shardCount) yield Lines.fromFile(s"$basename#$s$suffix").ofSeq(converter)
//    )
//  }

  def bucketLevelValue[T](basename: String, suffix: String)(implicit converter: String => T): Iterator[Seq[Seq[T]]] = {
    val properties = Properties.get(basename)
    val shardCount = Features.get(properties).shardCount
    val bucketCount = properties.bucketCount
    new ZippedIterator(
      for (s <- 0 until shardCount) yield
        new ZippedIterator(
          for (b <- 0 until bucketCount) yield Lines.fromFile(s"$basename#$s#$b$suffix").of(converter)
        )
    )
  }

  def bucketLevelSequence[T](basename: String, suffix: String)(implicit converter: String => T): Iterator[Seq[Seq[Seq[T]]]] = {
    val properties = Properties.get(basename)
    val shardCount = Features.get(properties).shardCount
    val bucketCount = properties.bucketCount
    new ZippedIterator(
      for (s <- 0 until shardCount) yield
        new ZippedIterator(
          for (b <- 0 until bucketCount)
            yield Lines.fromFile(s"$basename#$s#$b$suffix").ofSeq(converter)
        )
    )
  }

  def selectedDocumentsAt(basename: String): Iterator[Seq[Long]] = queryLevelSequence(basename, s"$SelectedSuffix$DocumentsSuffix")
  def selectedScoresAt(basename: String): Iterator[Seq[Double]] = queryLevelSequence(basename, s"$SelectedSuffix$ScoresSuffix")

  def payoffsAt(basename: String): Iterator[Seq[Seq[Double]]] = bucketLevelValue(basename, PayoffSuffix)
  def costsAt(basename: String): Iterator[Seq[Seq[Double]]] = bucketLevelValue(basename, CostSuffix)
  def bucketizedGlobalResultsAt(basename: String): Iterator[Seq[Seq[Seq[Long]]]] = bucketLevelSequence(basename, s"$ResultsSuffix$GlobalSuffix")

//  def costsAt(basename: String, shardId: Int): Iterator[Double] = Lines.fromFile(s"$basename#$shardId.cost").of[Double]
//  def costs(basename: String, shardId: Int): Iterator[Double] = Lines.fromFile(s"$basename#$shardId.cost").of[Double]
//  def costs(basename: String): Iterator[Seq[Double]] =
//    ZippedIterator(for (s <- 0 until shardCount) yield Lines.fromFile(s"$basename#$s.cost").of[Double]).strict
}
