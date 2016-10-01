package edu.nyu.tandon.search.selective.data.results

/**
  * @author michal.siedlaczek@nyu.edu
  */
class Result(val documentId: Long, val score: Option[Double]) {
  def hasScore: Boolean = score.isDefined
  def scoreValue: Double = score.get
  override def equals(that: Any): Boolean = {
    that match {
      case r: Result => (this.documentId, this.score) == (r.documentId, r.score)
      case _ => false
    }
  }
}

object Result {
  def apply(documentId: Long) = new Result(documentId, None)
  def apply(documentId: Long, score: Double) = new Result(documentId, Some(score))
  def apply(documentId: Long, score: Option[Double]) = new Result(documentId, score)
}