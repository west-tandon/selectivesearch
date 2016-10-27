package edu.nyu.tandon.search.selective.data.features

import java.io.File

import edu.nyu.tandon._
import edu.nyu.tandon.search.selective.data.results.Result
import edu.nyu.tandon.utils.ZippedIterator

/**
  * @author michal.siedlaczek@nyu.edu
  */
class Features(basename: String) {

  lazy val lazyShardSizes = lines(s"$basename.sizes").map(_.toLong).toIndexedSeq

  /* Shards */
  def shardCount: Int = shardSizes.length
  def shardSize(id: Int): Long = shardSizes(id)
  def shardSizes: IndexedSeq[Long] = lazyShardSizes

  def reddeScores: Iterator[Seq[Double]] =
    ZippedIterator(for (s <- 0 until shardCount) yield lines(s"$basename#$s.redde")(_.toDouble)).strict
  def shrkcScores: Iterator[Seq[Double]] =
    ZippedIterator(for (s <- 0 until shardCount) yield lines(s"$basename#$s.shrkc")(_.toDouble)).strict

  def costs(shardId: Int): Iterator[Double] = lines(s"$basename#$shardId.cost")(_.toDouble)
  def costs: Iterator[Seq[Double]] =
    ZippedIterator(for (s <- 0 until shardCount) yield lines(s"$basename#$s.cost")(_.toDouble)).strict

  def maxListLen1: Iterator[Seq[Double]] =
    ZippedIterator(for (s <- 0 until shardCount) yield lines(s"$basename#$s.maxlist1")(_.toDouble)).strict
  def maxListLen2: Iterator[Seq[Double]] =
    ZippedIterator(for (s <- 0 until shardCount) yield lines(s"$basename#$s.maxlist2")(_.toDouble)).strict
  def minListLen1: Iterator[Seq[Double]] =
    ZippedIterator(for (s <- 0 until shardCount) yield lines(s"$basename#$s.minlist1")(_.toDouble)).strict
  def minListLen2: Iterator[Seq[Double]] =
    ZippedIterator(for (s <- 0 until shardCount) yield lines(s"$basename#$s.minlist2")(_.toDouble)).strict
  def sumListLen: Iterator[Seq[Double]] =
    ZippedIterator(for (s <- 0 until shardCount) yield lines(s"$basename#$s.sumlist")(_.toDouble)).strict

  /* Queries */
  def queries: Iterator[String] = lines(s"$basename.queries")
  def queryLengths: Iterator[Int] = lines(s"$basename.lengths").map(_.toInt)
  def trecIds: Iterator[Long] = lines(s"$basename.trecid").map(_.toLong)

  /* Documents */
  def documentTitles: Iterator[String] = lines(s"$basename.titles")
  def baseResults: Iterator[Seq[Long]] = lines(s"$basename.results.global")(lineToLongs)
  def shardResults: Iterator[IndexedSeq[Seq[Result]]] =
    ZippedIterator(
      for (s <- 0 until shardCount) yield {
        val localIds = lines(s"$basename#$s.results.local")(lineToLongs)
        val globalIds = lines(s"$basename#$s.results.global")(lineToLongs)
        val scores = lines(s"$basename#$s.results.scores")(lineToDoubles)
        for (((l, g), s) <- localIds.zip(globalIds).zip(scores)) yield {
          require(l.length == g.length && g.length == s.length,
            s"different number of elements in a line among (local, global and scores) = (${l.length}, ${g.length}, ${s.length})")
          for (((localId, globalId), score) <- l.zip(g).zip(s)) yield Result(localId, globalId, score)
        }
      }
    ).strict.map(_.toIndexedSeq)
  def shardResults(shardId: Int): Iterator[Seq[Result]] = {
    val localIds = lines(s"$basename#$shardId.results.local")(lineToLongs)
    val globalIds = lines(s"$basename#$shardId.results.global")(lineToLongs)
    val scores = lines(s"$basename#$shardId.results.scores")(lineToDoubles)
    for (((l, g), s) <- localIds.zip(globalIds).zip(scores)) yield {
      require(l.length == g.length && g.length == s.length,
        s"different number of elements in a line among (local, global and scores) = (${l.length}, ${g.length}, ${s.length})")
      for (((localId, globalId), score) <- l.zip(g).zip(s)) yield Result(localId, globalId, score)
    }
  }

}

object Features {
  def get(basename: String): Features = {
    val path = loadProperties(basename).getProperty("features")
    if (new File(path).isAbsolute) new Features(path)
    else new Features(s"${new File(basename).getAbsoluteFile.getParent}/$path")
  }
}