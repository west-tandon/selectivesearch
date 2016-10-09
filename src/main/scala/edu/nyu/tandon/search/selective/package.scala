package edu.nyu.tandon.search

import edu.nyu.tandon._
import edu.nyu.tandon.utils.ZippableSeq
import scala.language.implicitConversions

import scalax.io.Resource

/**
  * @author michal.siedlaczek@nyu.edu
  */
package object selective {

  val CostSuffix = ".cost"
  val DivisionSuffix = ".division"
  val PayoffSuffix = ".payoff"
  val QueriesSuffix = ".queries"
  val QueryLengthsSuffix = ".qlen"
  val SelectionSuffix = ".selection"
  val SelectedSuffix = ".selected"
  val ResultsSuffix = ".results"
  val DocumentsSuffix = ".docs"
  val LocalSuffix = ".local"
  val GlobalSuffix = ".global"
  val ScoresSuffix = ".scores"
  val TrecSuffix = ".trec"
  val TrecIdSuffix = ".trecid"
  val TitlesSuffix = ".titles"
  val ReDDESuffix = ".redde"
  val ShRkCSuffix = ".shrkc"
  val LabelSuffix = ".label"
  val ModelSuffix = ".model"
  val EvalSuffix = ".eval"

  val FieldSplitter = "\\s+"
  val FieldSeparator = " "

  val NestingIndicator = "#"

  def base(nestedBasename: String): String = nestedBasename.takeWhile(c => s"$c" != NestingIndicator)

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

}
