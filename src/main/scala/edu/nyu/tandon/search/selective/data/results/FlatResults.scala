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
    storeStrings(Path.toQueries(basename), queries)
    storeStrings(Path.toLocalResults(basename), localDocumentIds)
    storeStrings(Path.toGlobalResults(basename), globalDocumentIds)
    if (hasScores) storeStrings(Path.toScores(basename), scores.map(_.get))
  }

  def storeStrings(file: String, strings: Iterable[String]): Unit = {
    val writer = new FileWriter(file)
    for (s <- strings) writer.append(s"$s\n")
    writer.close()
  }
}

object FlatResults {

  def fromBasename(basename: String): FlatResults = {
    val queries = Load.queriesAt(base(basename))
    val localDocs = lines(Path.toLocalResults(basename))
    val globalDocs = lines(Path.toGlobalResults(basename))

    Files.exists(Paths.get(Path.toScores(basename))) match {
      case true =>
        val scores = Source.fromFile(Path.toScores(basename)).getLines().toIterable
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