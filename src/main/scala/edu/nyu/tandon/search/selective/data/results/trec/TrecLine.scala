package edu.nyu.tandon.search.selective.data.results.trec

/**
  * @author michal.siedlaczek@nyu.edu
  */
class TrecLine(val queryId: Long,
               val topic: String,
               val title: String,
               val index: Int,
               val score: Double) {
  def apply(queryId: Long, topic: String, title: String, index: Int, score: Double) = new TrecLine(queryId, topic, title, index, score)
  override def toString: String = s"$queryId\t$topic\t$title\t$index\t$score\tnull"
}
