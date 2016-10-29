package edu.nyu.tandon.search.selective.data.results

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective.data.Properties
import edu.nyu.tandon.search.selective.data.features.Features

/**
  * @author michal.siedlaczek@nyu.edu
  */
class GroupedGroupedResults(val iterator: Iterator[Seq[Seq[ResultLine]]])
  extends Iterator[Seq[Seq[ResultLine]]] with LazyLogging {

  override def hasNext: Boolean = iterator.hasNext
  override def next(): Seq[Seq[ResultLine]] = iterator.next()

  def store(basename: String): Unit = {

    val properties = Properties.get(basename)
    val features = Features.get(properties)

    val shardCount = features.shardCount
    val bucketCount = properties.bucketCount

    val writers =
      for (s <- 0 until shardCount) yield
        for (b <- 0 until bucketCount) yield FlatResults.writers(s"$basename#$s#$b")

    for ((resultLinesForQuery, i) <- iterator.zipWithIndex) {
      logger.info(s"Processing query $i")
      for ((shardResults, shardWriters) <- resultLinesForQuery zip writers)
        for ((resultLine, (lw, gw, sw)) <- shardResults zip shardWriters)
          FlatResults.writeLine(lw, gw, sw, resultLine)
    }

    for (shardWriters <- writers)
      for ((lw, gw, sw) <- shardWriters)
        FlatResults.closeWriters(lw, gw, sw)
  }

  def select(selector: Iterable[Seq[Int]]): Iterable[Seq[Result]] =
    (for (((results, i), selection) <- iterator.zipWithIndex zip selector.iterator) yield {
      logger.info(s"Processing query $i")
      (for ((shardResults, numOfBuckets) <- results zip selection) yield
        shardResults.take(numOfBuckets).flatten).flatten.sortBy(-_.score)
    }).toIterable

}
