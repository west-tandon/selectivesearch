package edu.nyu.tandon.search.selective.data.results

import java.io.FileWriter
import java.nio.file.{Files, Paths}

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective._
import edu.nyu.tandon.search.selective.data.results.FlatResults.{closeWriters, writeLine, writers}
import edu.nyu.tandon.{base => _, _}

/**
  * @author michal.siedlaczek@nyu.edu
  */
class FlatResults(val iterator: Iterator[ResultLine], val hasScores: Boolean) extends Iterator[ResultLine] with LazyLogging {

  override def hasNext: Boolean = iterator.hasNext
  override def next(): ResultLine = iterator.next()

  def bucketize(bucketSize: Long, bucketCount: Int): GroupedResults =
    new GroupedResults(
      iterator.map(_.groupByBuckets(bucketSize, bucketCount)),
      bucketCount,
      hasScores
    )

  def store(basename: String): Unit = {

    logger.debug(s"Storing results to $basename")

    val (qw, lw, gw, sw) = writers(basename, hasScores)
    for (resultLine <- iterator) writeLine(qw, lw, gw, sw, resultLine)
    closeWriters(qw, lw, gw, sw)

  }

}

object FlatResults extends LazyLogging {

  def writers(basename: String, hasScores: Boolean): (FileWriter, FileWriter, FileWriter, Option[FileWriter]) = (
    new FileWriter(Path.toQueries(basename)),
    new FileWriter(Path.toLocalResults(basename)),
    new FileWriter(Path.toGlobalResults(basename)),
    if (hasScores) Some(new FileWriter(Path.toScores(basename))) else None
  )

  def closeWriters(qw: FileWriter, lw: FileWriter, gw: FileWriter, sw: Option[FileWriter]) = {
    qw.close()
    lw.close()
    gw.close()
    sw match {
      case Some(w) => w.close()
      case None =>
    }
  }

  def writeLine(qw: FileWriter, lw: FileWriter, gw: FileWriter, sw: Option[FileWriter], resultLine: ResultLine) = {
    val (q, l, g, s) = resultLine.toStringTuple
    logger.trace(s"Writing result line ($q, $l, $g, $s)")
    qw.append(s"$q\n")
    lw.append(s"$l\n")
    gw.append(s"$g\n")
    sw match {
      case Some(w) => w.append(s"${s.get}\n")
      case None =>
    }
  }

  def fromBasename(basename: String): FlatResults = {

    val queries = Load.queriesAt(base(basename))
    val localDocs = lines(Path.toLocalResults(basename))
    val globalDocs = lines(Path.toGlobalResults(basename))

    Files.exists(Paths.get(Path.toScores(basename))) match {
      case true =>
        val scores = lines(Path.toScores(basename))
        new FlatResults(((queries zip localDocs) zip (globalDocs zip scores)).map {
          case ((q, l), (g, s)) =>
            logger.trace(s"Reading result line ($q, $l, $g, $s)")
            ResultLine.fromString(query = q, localDocumentIds = l, globalDocumentIds = g, scores = s)
        }, true)
      case false =>
        new FlatResults(((queries zip localDocs) zip globalDocs).map {
          case ((q, l), g) => ResultLine.fromString(query = q, localDocumentIds = l, globalDocumentIds = g)
        }, false)
    }
  }

}