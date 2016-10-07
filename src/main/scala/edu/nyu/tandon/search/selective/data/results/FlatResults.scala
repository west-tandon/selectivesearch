package edu.nyu.tandon.search.selective.data.results

import java.io.FileWriter
import java.nio.file.{Files, Paths}

import edu.nyu.tandon.search.selective._

import scala.io.Source

/**
  * @author michal.siedlaczek@nyu.edu
  */
class FlatResults(val sequence: Iterable[ResultLine], val hasScores: Boolean) extends Iterable[ResultLine] {

  override def iterator: Iterator[ResultLine] = sequence.iterator

  def partition(partitionSize: Long, partitionCount: Int): GroupedResults = {
    val x = this map (_.groupByBuckets(partitionSize, partitionCount))
    new GroupedResults(
      for (b <- 0 until partitionCount)
        yield {
          new FlatResults(this.map(_.groupByBuckets(partitionSize, partitionCount)(b)), hasScores)
        }
    )
  }

  def store(basename: String): Unit = {
    val (queries, documentIds, scores) = this.map(_.toStringTuple).unzip3
    storeQueries(basename, queries)
    storeDocumentIds(basename, documentIds)
    if (hasScores) storeScores(basename, scores.map(_.get))
  }
  def storeQueries(basename: String, queries: Iterable[String]): Unit =
    storeStrings(s"${base(basename)}$QueriesSuffix", queries)
  def storeDocumentIds(basename: String, documentIds: Iterable[String]): Unit =
    storeStrings(s"$basename$ResultsSuffix", documentIds)
  def storeScores(basename: String, scores: Iterable[String]): Unit =
    storeStrings(s"$basename$ScoresSuffix", scores)
  def storeStrings(file: String, strings: Iterable[String]): Unit = {
    val writer = new FileWriter(file)
    for (s <- strings) writer.append(s"$s\n")
    writer.close()
  }
}

object FlatResults {

  def fromBasename(basename: String): FlatResults = {
    val b = base(basename)
    val queries = Source.fromFile(s"${base(basename)}$QueriesSuffix").getLines().toIterable
    val documents = Source.fromFile(s"$basename$ResultsSuffix").getLines().toIterable

    Files.exists(Paths.get(s"$basename$ScoresSuffix")) match {
      case true =>
        val scores = Source.fromFile(s"$basename$ScoresSuffix").getLines().toIterable
        new FlatResults((queries.toList, documents, scores).zipped.map {
          case (q, d, s) => ResultLine.fromString(q, d, s)
        }, true)
      case false =>
        new FlatResults((queries, documents).zipped.map {
          case (q, d) => ResultLine.fromString(q, d)
        }, false)
    }
  }

}