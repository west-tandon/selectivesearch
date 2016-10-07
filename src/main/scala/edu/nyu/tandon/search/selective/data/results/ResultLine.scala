package edu.nyu.tandon.search.selective.data.results

import edu.nyu.tandon.search.selective._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class ResultLine(val query: String,
                 val documentIds: Seq[Long],
                 scores: Option[Seq[Double]]) extends Iterable[Result] {

  def apply(query: String, documentIds: Seq[Long], scores: Option[Seq[Double]]) = new ResultLine(query, documentIds, scores)

  override def iterator: Iterator[Result] = {

    val documentIterator = documentIds.iterator

    scores match {
      case None =>
        new Iterator[Result] {
          override def hasNext: Boolean = documentIterator.hasNext
          override def next(): Result = Result(documentIterator.next())
        }
      case Some(scoreSeq) =>
        val scoreIterator = scoreSeq.iterator
        new Iterator[Result] {
          override def hasNext: Boolean = {
            require(documentIterator.hasNext == scoreIterator.hasNext,
              "unexpected end of one of the iterators: " +
                s"documentIterator.hasNext = ${documentIterator.hasNext}; " +
                s"scoreIterator.hasNext = ${scoreIterator.hasNext}")
            documentIterator.hasNext
          }
          override def next(): Result = Result(documentIterator.next(), scoreIterator.next())
        }
    }

  }

  def groupByBuckets(partitionSize: Long, partitionCount: Int): Seq[ResultLine] = {
    val m = this.groupBy(_.documentId / partitionSize).mapValues(_.toSeq).withDefaultValue(Seq())
    for (i <- 0 until partitionCount)
      yield new ResultLine(query,
        m(i).map(_.documentId),
        scores match {
          case None => None
          case Some(s) => Some(m(i).map(_.scoreValue))
        }
    )
  }

  def toStringTuple: (String, String, Option[String]) = (query,
    documentIds.mkString(FieldSeparator),
    scores match {
      case None => None
      case Some(seq) => Some(seq.mkString(FieldSeparator))
    })

  def hasScores: Boolean = scores.isDefined

  def getScores: Seq[Double] = scores.get

}

object ResultLine {

  def fromString(query: String, documentIds: String, scores: String): ResultLine = new ResultLine(
    query,
    documentIds.split(FieldSplitter).map(_.toLong),
    Some(scores.split(FieldSplitter).map(_.toDouble))
  )

  def fromString(query: String, documentIds: String): ResultLine = new ResultLine(
    query,
    documentIds.split(FieldSplitter).map(_.toLong),
    None
  )

}
