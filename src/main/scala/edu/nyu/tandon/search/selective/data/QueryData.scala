package edu.nyu.tandon.search.selective.data

/**
  * @author michal.siedlaczek@nyu.edu
  */
case class QueryData(query: String,
                     bins: List[Bin]) extends Iterable[Bin] {
  def apply(query: String, bins: List[Bin]) = new QueryData(query, bins)
  override def iterator: Iterator[Bin] = bins.iterator
}
