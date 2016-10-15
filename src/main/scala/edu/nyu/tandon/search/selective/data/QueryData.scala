package edu.nyu.tandon.search.selective.data

/**
  * @author michal.siedlaczek@nyu.edu
  */
case class QueryData(bucketsByShard: Seq[List[Bucket]]) extends Iterable[List[Bucket]] {
  def apply(shards: Seq[List[Bucket]]) = new QueryData(shards)
  override def iterator: Iterator[List[Bucket]] = bucketsByShard.iterator
}
