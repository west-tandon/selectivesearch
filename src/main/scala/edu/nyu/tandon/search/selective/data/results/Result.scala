package edu.nyu.tandon.search.selective.data.results

/**
  * @author michal.siedlaczek@nyu.edu
  */
class Result(val localDocumentId: Long, val globalDocumentId: Long, val score: Double) {
  override def equals(that: Any): Boolean = {
    that match {
      case r: Result => (this.localDocumentId, this.globalDocumentId, this.score) == (r.localDocumentId, r.globalDocumentId, r.score)
      case _ => false
    }
  }
  override def toString: String = s"Result($localDocumentId, $globalDocumentId, $score)"
}

object Result {
  def apply(localDocumentId: Long, globalDocumentId: Long, score: Double) = new Result(localDocumentId, globalDocumentId, score)
}