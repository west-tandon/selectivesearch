package edu.nyu.tandon.search.selective.data.results

import edu.nyu.tandon.search.selective._
import edu.nyu.tandon._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class ResultLine(val query: String,
                 val localDocumentIds: Seq[Long],
                 val globalDocumentIds: Seq[Long],
                 scores: Option[Seq[Double]]) extends Iterable[Result] {

  def apply(query: String, localDocumentIds: Seq[Long], globalDocumentIds: Seq[Long], scores: Option[Seq[Double]]) =
    new ResultLine(query, localDocumentIds, globalDocumentIds, scores)

  override def iterator: Iterator[Result] = {

    val localDocumentIterator = localDocumentIds.iterator
    val globalDocumentIterator = globalDocumentIds.iterator

    scores match {
      case None =>
        new Iterator[Result] {
          override def hasNext: Boolean = {
            require(localDocumentIterator.hasNext == globalDocumentIterator.hasNext,
              "unexpected end of one of the iterators: " +
                s"localDocumentIterator.hasNext = ${localDocumentIterator.hasNext}; " +
                s"globalDocumentIterator.hasNext = ${globalDocumentIterator.hasNext}")
            localDocumentIterator.hasNext
          }
          override def next(): Result = Result(localDocumentIterator.next(), globalDocumentIterator.next())
        }
      case Some(scoreSeq) =>
        val scoreIterator = scoreSeq.iterator
        new Iterator[Result] {
          override def hasNext: Boolean = {
            require(localDocumentIterator.hasNext == scoreIterator.hasNext,
              "unexpected end of one of the iterators: " +
                s"documentIterator.hasNext = ${localDocumentIterator.hasNext}; " +
                s"scoreIterator.hasNext = ${scoreIterator.hasNext}")
            localDocumentIterator.hasNext
          }
          override def next(): Result =
            Result(localDocumentIterator.next(), globalDocumentIterator.next(), scoreIterator.next())
        }
    }

  }

  def groupByBuckets(bucketSize: Long, bucketCount: Int): Seq[ResultLine] = {
    val m = this.groupBy(_.localDocumentId / bucketSize).mapValues(_.toSeq).withDefaultValue(Seq())
    for (i <- 0 until bucketCount)
      yield new ResultLine(query,
        m(i).map(_.localDocumentId),
        m(i).map(_.globalDocumentId),
        scores match {
          case None => None
          case Some(s) => Some(m(i).map(_.scoreValue))
        }
    )
  }

  def toStringTuple: (String, String, String, Option[String]) = (query,
    localDocumentIds.mkString(FieldSeparator),
    globalDocumentIds.mkString(FieldSeparator),
    scores match {
      case None => None
      case Some(seq) => Some(seq.mkString(FieldSeparator))
    })

  def hasScores: Boolean = scores.isDefined

  def getScores: Seq[Double] = scores.get

}

object ResultLine {

  def fromString(query: String, localDocumentIds: String, globalDocumentIds: String, scores: String): ResultLine = new ResultLine(
    query,
    lineToLongs(localDocumentIds),
    lineToLongs(globalDocumentIds),
    Some(lineToDoubles(scores))
  )

  def fromString(query: String, localDocumentIds: String, globalDocumentIds: String): ResultLine = new ResultLine(
    query,
    lineToLongs(localDocumentIds),
    lineToLongs(globalDocumentIds),
    None
  )

}
