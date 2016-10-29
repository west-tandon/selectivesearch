package edu.nyu.tandon.search.selective.data.results

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

}

object ResultLine {

  def get(localDocumentIds: Seq[Long], globalDocumentIds: Seq[Long], scores: Seq[Double]): ResultLine =
    new ResultLine(
      for (((localId, globalId), score) <- localDocumentIds.zip(globalDocumentIds).zip(scores))
        yield Result(localId, globalId, score)
    )

}
