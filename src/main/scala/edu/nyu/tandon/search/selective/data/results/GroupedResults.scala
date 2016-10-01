package edu.nyu.tandon.search.selective.data.results

import edu.nyu.tandon.utils.BulkIterator

/**
  * @author michal.siedlaczek@nyu.edu
  */
class GroupedResults(val sequence: Seq[FlatResults]) extends Iterable[Seq[ResultLine]] {
  override def iterator: Iterator[Seq[ResultLine]] = new BulkIterator[ResultLine](sequence.map(_.iterator))
  def store(basename: String): Unit = for ((bin, b) <- sequence.zipWithIndex) bin.store(s"$basename#$b")
  def partition(partitionSize: Long, partitionCount: Int): GroupedGroupedResults =
    new GroupedGroupedResults(sequence map (_.partition(partitionSize, partitionCount)))
  def hasScores: Boolean = sequence.head.hasScores
}
