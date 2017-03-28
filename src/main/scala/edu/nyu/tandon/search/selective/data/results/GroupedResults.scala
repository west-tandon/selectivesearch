package edu.nyu.tandon.search.selective.data.results

import com.typesafe.scalalogging.LazyLogging

/**
  * @author michal.siedlaczek@nyu.edu
  */
class GroupedResults(val iterator: Iterator[Seq[ResultLine]], val groups: Int)
  extends Iterator[Seq[ResultLine]] with LazyLogging {

  override def hasNext: Boolean = iterator.hasNext
  override def next(): Seq[ResultLine] = iterator.next

  def store(basename: String): Int = {

    val writers = for (s <- 0 until groups)
      yield FlatResults.writers(s"$basename#$s")

    var count = 0
    for ((resultLinesForQuery, i) <- iterator.zipWithIndex) {
      count += 1
      for ((resultLine, (lw, gw, sw)) <- resultLinesForQuery zip writers)
        FlatResults.writeLine(lw, gw, sw, resultLine)
    }

    for ((lw, gw, sw) <- writers) FlatResults.closeWriters(lw, gw, sw)

    count
  }

  def bucketize(bucketSizes: Seq[Long], bucketCount: Int): GroupedGroupedResults = {
    new GroupedGroupedResults(
      for (queryResults <- iterator) yield {
        for ((shardResults, bucketSize) <- queryResults zip bucketSizes) yield {
          shardResults.groupByBuckets(bucketSize, bucketCount)
        }
      }
    )
  }

}
