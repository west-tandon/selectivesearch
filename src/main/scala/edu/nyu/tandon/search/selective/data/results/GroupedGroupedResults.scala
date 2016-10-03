package edu.nyu.tandon.search.selective.data.results

import edu.nyu.tandon.utils.BulkIterator

/**
  * @author michal.siedlaczek@nyu.edu
  */
class GroupedGroupedResults(val sequence: Seq[GroupedResults]) extends Iterable[Seq[Seq[ResultLine]]] {
  override def iterator: Iterator[Seq[Seq[ResultLine]]] = new BulkIterator[Seq[ResultLine]](sequence.map(_.iterator))
  def hasScores: Boolean = sequence.head.hasScores
  def store(basename: String): Unit = for ((shard, s) <- sequence.zipWithIndex) shard.store(s"$basename#$s")
  def select(selector: Iterable[Seq[Int]]): Iterable[Seq[Result]] =
    for ((results, selection) <- this zip selector) yield
      (for ((shardResults, numOfBins) <- results zip selection) yield
        shardResults.take(numOfBins).flatten).flatten.sortBy(_.score match {
        case None => 0
        case Some(s) => -s
      })
}
