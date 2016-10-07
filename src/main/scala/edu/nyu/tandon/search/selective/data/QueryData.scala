package edu.nyu.tandon.search.selective.data

/**
  * @author michal.siedlaczek@nyu.edu
  */
case class QueryData(query: String,
                     bucketsByShard: Seq[List[Bucket]]) extends Iterable[List[Bucket]] {
  def apply(query: String, shards: Seq[List[Bucket]]) = new QueryData(query, shards)
  override def iterator: Iterator[List[Bucket]] = bucketsByShard.iterator
}
