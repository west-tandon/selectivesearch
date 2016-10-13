package edu.nyu.tandon.search.selective.data.results

import com.typesafe.scalalogging.LazyLogging

/**
  * @author michal.siedlaczek@nyu.edu
  */
class GroupedResults(val iterator: Iterator[Seq[ResultLine]], val groups: Int, val hasScores: Boolean)
  extends Iterator[Seq[ResultLine]] with LazyLogging {

  override def hasNext: Boolean = iterator.hasNext
  override def next(): Seq[ResultLine] = iterator.next

  def store(basename: String): Unit = {

    val writers = for (s <- 0 until groups)
      yield FlatResults.writers(s"$basename#$s", hasScores)

    for ((resultLinesForQuery, i) <- iterator.zipWithIndex) {
      logger.info(s"Processing query $i")
      for ((resultLine, (qw, lw, gw, sw)) <- resultLinesForQuery zip writers)
        FlatResults.writeLine(qw, lw, gw, sw, resultLine)
    }

    for ((qw, lw, gw, sw) <- writers) FlatResults.closeWriters(qw, lw, gw, sw)

  }

  def bucketize(bucketSizes: Seq[Long], bucketCount: Int): GroupedGroupedResults = {
    new GroupedGroupedResults(
      for (queryResults <- iterator) yield {
        for ((shardResults, bucketSize) <- queryResults zip bucketSizes) yield {
          shardResults.groupByBuckets(bucketSize, bucketCount)
        }
      },
      hasScores
    )
  }

}
