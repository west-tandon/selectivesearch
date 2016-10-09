package edu.nyu.tandon.search.selective.data.results

/**
  * @author michal.siedlaczek@nyu.edu
  */
class Result(val localDocumentId: Long, val globalDocumentId: Long, val score: Option[Double]) {
  def hasScore: Boolean = score.isDefined
  def scoreValue: Double = score.get
  override def equals(that: Any): Boolean = {
    that match {
      case r: Result => (this.localDocumentId, this.globalDocumentId, this.score) == (r.localDocumentId, r.globalDocumentId, r.score)
      case _ => false
    }
  }
}

object Result {
  def apply(localDocumentId: Long, globalDocumentId: Long) = new Result(localDocumentId, globalDocumentId, None)
  def apply(localDocumentId: Long, globalDocumentId: Long, score: Double) = new Result(localDocumentId, globalDocumentId, Some(score))
  def apply(localDocumentId: Long, globalDocumentId: Long, score: Option[Double]) = new Result(localDocumentId, globalDocumentId, score)
}