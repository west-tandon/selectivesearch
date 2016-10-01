package edu.nyu.tandon.search.selective.data.results

/**
  * @author michal.siedlaczek@nyu.edu
  */
class GroupedGroupedResults(sequence: Iterable[GroupedResults]) extends Iterable[GroupedResults] {
  override def iterator: Iterator[GroupedResults] = sequence.iterator
  def store(basename: String): Unit = for ((shard, s) <- this.zipWithIndex) shard.store(s"$basename#$s")
}
