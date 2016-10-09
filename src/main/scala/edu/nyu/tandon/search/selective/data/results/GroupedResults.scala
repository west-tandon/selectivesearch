package edu.nyu.tandon.search.selective.data.results

import edu.nyu.tandon.utils.BulkIterator

/**
  * @author michal.siedlaczek@nyu.edu
  */
class GroupedResults(val sequence: Seq[FlatResults]) extends Iterable[Seq[ResultLine]] {
  override def iterator: Iterator[Seq[ResultLine]] = new BulkIterator[ResultLine](sequence.map(_.iterator))
  def store(basename: String): Unit = for ((bucket, b) <- sequence.zipWithIndex) bucket.store(s"$basename#$b")
  def bucketize(bucketSizes: Seq[Long]): GroupedGroupedResults = {
    require(sequence.length == bucketSizes.length, "discrepancy in shard counts")
    new GroupedGroupedResults(
      for ((shard, bucketSize) <- sequence zip bucketSizes) yield shard.bucketize(bucketSize, bucketSizes.length)
    )
  }
  def hasScores: Boolean = sequence.head.hasScores
}
