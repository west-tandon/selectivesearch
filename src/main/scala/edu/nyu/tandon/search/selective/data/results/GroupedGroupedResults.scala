package edu.nyu.tandon.search.selective.data.results

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class GroupedGroupedResults(val iterator: Iterator[Seq[Seq[ResultLine]]], val hasScores: Boolean)
  extends Iterator[Seq[Seq[ResultLine]]] with LazyLogging {

  override def hasNext: Boolean = iterator.hasNext
  override def next(): Seq[Seq[ResultLine]] = iterator.next()

  def store(basename: String): Unit = {
    val shardCount = loadProperties(basename).getProperty("shards.count").toInt
    val bucketCount = loadProperties(basename).getProperty("buckets.count").toInt

    val writers =
      for (s <- 0 until shardCount) yield
        for (b <- 0 until bucketCount) yield FlatResults.writers(s"$basename#$s#$b", hasScores)

    for ((resultLinesForQuery, i) <- iterator.zipWithIndex) {
      logger.info(s"Processing query $i")
      for ((shardResults, shardWriters) <- resultLinesForQuery zip writers)
        for ((resultLine, (qw, lw, gw, sw)) <- shardResults zip shardWriters)
          FlatResults.writeLine(qw, lw, gw, sw, resultLine)
    }

    for (shardWriters <- writers)
      for ((qw, lw, gw, sw) <- shardWriters)
        FlatResults.closeWriters(qw, lw, gw, sw)
  }

  def select(selector: Iterable[Seq[Int]]): Iterable[Seq[Result]] =
    (for (((results, i), selection) <- iterator.zipWithIndex zip selector.iterator) yield {
      logger.info(s"Processing query $i")
      (for ((shardResults, numOfBuckets) <- results zip selection) yield
        shardResults.take(numOfBuckets).flatten).flatten.sortBy(_.score match {
        case None => 0
        case Some(s) => -s
      })
    }).toIterable

}
