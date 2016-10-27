package edu.nyu.tandon.search.selective.data.results

import java.io.FileWriter

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective._
import edu.nyu.tandon.search.selective.data.features.Features
import edu.nyu.tandon.search.selective.data.results.FlatResults.{closeWriters, writeLine, writers}
import edu.nyu.tandon._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class FlatResults(val iterator: Iterator[ResultLine]) extends Iterator[ResultLine] with LazyLogging {

  override def hasNext: Boolean = iterator.hasNext
  override def next(): ResultLine = iterator.next()

  def bucketize(bucketSize: Long, bucketCount: Int): GroupedResults =
    new GroupedResults(
      iterator.map(_.groupByBuckets(bucketSize, bucketCount)),
      bucketCount
    )

  def store(basename: String): Unit = {

    logger.debug(s"Storing results to $basename")

    val (lw, gw, sw) = writers(basename)
    for (resultLine <- iterator) writeLine(lw, gw, sw, resultLine)
    closeWriters(lw, gw, sw)

  }

}

object FlatResults extends LazyLogging {

  def writers(basename: String): (FileWriter, FileWriter, FileWriter) = (
    new FileWriter(Path.toLocalResults(basename)),
    new FileWriter(Path.toGlobalResults(basename)),
    new FileWriter(Path.toScores(basename))
  )

  def closeWriters(lw: FileWriter, gw: FileWriter, sw: FileWriter) = {
    lw.close()
    gw.close()
    sw.close()
  }

  def writeLine(lw: FileWriter, gw: FileWriter, sw: FileWriter, resultLine: ResultLine) = {
    val (local, global, scores) = resultLine.map((r) => (r.localDocumentId, r.globalDocumentId, r.score)).unzip3
    lw.append(s"${local.mkString(" ")}\n")
    gw.append(s"${global.mkString(" ")}\n")
    sw.append(s"${scores.mkString(" ")}\n")
  }

  def fromBasename(basename: String): FlatResults = {

    val localDocs = lines(Path.toLocalResults(basename))
    val globalDocs = lines(Path.toGlobalResults(basename))

    val scores = lines(Path.toScores(basename))
    new FlatResults((localDocs zip (globalDocs zip scores)).map {
      case (l, (g, s)) =>
        logger.trace(s"Reading result line ($l, $g, $s)")
        ResultLine.fromString(localDocumentIds = l, globalDocumentIds = g, scores = s)
    })
  }

  def fromFeatures(features: Features, shardId: Int, k: Int): FlatResults = {
    new FlatResults(features.shardResults(shardId).map((l) => ResultLine(l.take(k))))
  }

}