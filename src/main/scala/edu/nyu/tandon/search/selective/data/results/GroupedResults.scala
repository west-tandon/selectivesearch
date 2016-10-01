package edu.nyu.tandon.search.selective.data.results

import edu.nyu.tandon._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class GroupedResults(sequence: Iterable[FlatResults]) extends Iterable[FlatResults] {
  override def iterator: Iterator[FlatResults] = sequence.iterator
  def store(basename: String): Unit = for ((bin, b) <- this.zipWithIndex) bin.store(s"$basename#$b")
  def partition(partitionSize: Long, partitionCount: Int): GroupedGroupedResults =
    new GroupedGroupedResults(this map (_.partition(partitionSize, partitionCount)))
}
