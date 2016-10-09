package edu.nyu.tandon.search.selective.data.results

import java.io.FileWriter
import java.nio.file.{Files, Paths}

import edu.nyu.tandon.search.selective._
import edu.nyu.tandon.{base => _, _}

import scala.io.Source

/**
  * @author michal.siedlaczek@nyu.edu
  */
class FlatResults(val sequence: Iterable[ResultLine], val hasScores: Boolean) extends Iterable[ResultLine] {

  override def iterator: Iterator[ResultLine] = sequence.iterator

  def bucketize(bucketSize: Long, bucketCount: Int): GroupedResults =
    new GroupedResults(
      for (b <- 0 until bucketCount) yield
          new FlatResults(this.map(_.groupByBuckets(bucketSize, bucketCount)(b)), hasScores)
    )

  def store(basename: String): Unit = {
    val ((queries, localDocumentIds), (globalDocumentIds, scores)): ((Iterable[String], Iterable[String]), (Iterable[String], Iterable[Option[String]])) = {
      val mid = this.map(_.toStringTuple).unzip { case (a, b, c, d) => ((a, b), (c, d)) }
      (mid._1.unzip, mid._2.unzip)
    }
    storeQueries(basename, queries)
    storeDocumentIds(basename, localDocumentIds, LocalSuffix)
    storeDocumentIds(basename, globalDocumentIds, GlobalSuffix)
    if (hasScores) storeScores(basename, scores.map(_.get))
  }
  def storeQueries(basename: String, queries: Iterable[String]): Unit =
    storeStrings(s"${base(basename)}$QueriesSuffix", queries)
  def storeDocumentIds(basename: String, documentIds: Iterable[String], suffix: String): Unit =
    storeStrings(s"$basename$ResultsSuffix$suffix", documentIds)
  def storeScores(basename: String, scores: Iterable[String]): Unit =
    storeStrings(s"$basename$ResultsSuffix$ScoresSuffix", scores)
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
    val localDocs = Source.fromFile(s"$basename$ResultsSuffix$LocalSuffix").getLines().toIterable
    val globalDocs = Source.fromFile(s"$basename$ResultsSuffix$GlobalSuffix").getLines().toIterable

    Files.exists(Paths.get(s"$basename$ResultsSuffix$ScoresSuffix")) match {
      case true =>
        val scores = Source.fromFile(s"$basename$ResultsSuffix$ScoresSuffix").getLines().toIterable
        new FlatResults(((queries.toList zip localDocs) zip (globalDocs zip scores)).map {
          case ((q, l), (g, s)) =>
            ResultLine.fromString(query = q, localDocumentIds = l, globalDocumentIds = g, scores = s)
        }, true)
      case false =>
        new FlatResults((queries, localDocs, globalDocs).zipped.map {
          case (q, l, g) => ResultLine.fromString(query = q, localDocumentIds = l, globalDocumentIds = g)
        }, false)
    }
  }

}