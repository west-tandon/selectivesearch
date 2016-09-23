package edu.nyu.tandon.search.selective.data

/**
  * @author michal.siedlaczek@nyu.edu
  */
case class QueryData(query: String,
                     binsByShard: Seq[List[Bin]]) extends Iterable[List[Bin]] {
  def apply(query: String, shards: Seq[List[Bin]]) = new QueryData(query, shards)
  override def iterator: Iterator[List[Bin]] = binsByShard.iterator
}
