package edu.nyu.tandon.search.selective.data.results

import edu.nyu.tandon._

/**
  * @author michal.siedlaczek@nyu.edu
  */
case class ResultLine(results: Seq[Result]) extends Iterable[Result] {

  def apply(results: Seq[Result]) = new ResultLine(results)

  override def iterator: Iterator[Result] = results.iterator

  def groupByBuckets(bucketSize: Long, bucketCount: Int): Seq[ResultLine] = {
    val m = results.groupBy(_.localDocumentId / bucketSize).withDefaultValue(Seq())
    for (i <- 0 until bucketCount) yield new ResultLine(m(i))
  }

//  def toStringTuple: (String, String, String) =
//    results.map((r) => (r.localDocumentId, r.globalDocumentId, r.score))

}

object ResultLine {

  def fromString(localDocumentIds: String, globalDocumentIds: String, scores: String): ResultLine =
    new ResultLine(
      for (((localId, globalId), score) <- lineToLongs(localDocumentIds).zip(lineToLongs(globalDocumentIds)).zip(lineToDoubles(scores)))
        yield Result(localId, globalId, score)
    )

}
