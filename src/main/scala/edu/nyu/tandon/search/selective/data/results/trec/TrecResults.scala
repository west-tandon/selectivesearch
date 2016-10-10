package edu.nyu.tandon.search.selective.data.results.trec

import edu.nyu.tandon.search.selective._

import scalax.io.Resource
import edu.nyu.tandon._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class TrecResults(val lines: Iterable[TrecLine]) extends Iterable[TrecLine] {
  override def iterator: Iterator[TrecLine] = lines.iterator

  /**
    * Store Trec Results to a file [basename].trec
    * @param basename the basename to which store the file
    */
  def store(basename: String): Unit = saveAs(s"$basename$TrecSuffix")

  /**
    * Save Trec Results in a file.
    * @param file output file
    */
  def saveAs(file: String): Unit = Resource.fromFile(file).writeStrings(lines.map(_.toString + "\n"))
}

object TrecResults {
  val TrecSeparator = "\t"
  val TrecSplitter = "\\s+"

  def fromSelected(basename: String): TrecResults = {
    val documentIds = lines(s"$basename$SelectedSuffix$DocumentsSuffix")(_.split(FieldSplitter).map(_.toLong))
    val scores = lines(s"$basename$SelectedSuffix$ScoresSuffix")(_.split(FieldSplitter).map(_.toDouble))
    val titles = lines(s"$basename$TitlesSuffix")
    val trecIds = lines(s"$basename$TrecIdSuffix")(_.toInt)

    new TrecResults(
      (for (
        ((qDocIds, qScores), qTrecId) <- documentIds.zip(scores).zip(trecIds);
        ((docId, score), i) <- qDocIds.zip(qScores).zipWithIndex
      ) yield new TrecLine(qTrecId, "Q0", titles(docId), i, score)).toIterable
    )
  }

  def fromTrecFile(trecFile: String): TrecResults =
    new TrecResults(Resource.fromFile(trecFile).lines()
      .map (line => {
        val s = line.split(TrecSplitter)
        new TrecLine(
          queryId = s(0).toInt,
          topic = s(1),
          title = s(2),
          index =  s(3).toInt,
          score = s(4).toDouble
        )
      }).toIterable)

}