package edu.nyu.tandon.search.selective.data.results.trec

import java.io.FileWriter

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon._
import edu.nyu.tandon.search.selective._
import edu.nyu.tandon.search.selective.data.features.Features

/**
  * @author michal.siedlaczek@nyu.edu
  */
class TrecResults(val lines: Iterator[TrecLine]) extends Iterator[TrecLine] {

  override def hasNext: Boolean = lines.hasNext
  override def next(): TrecLine = lines.next()

  /**
    * Store Trec Results to a file [basename].trec
    *
    * @param basename the basename to which store the file
    */
  def store(basename: String): Unit = saveAs(Path.toTrec(basename))

  /**
    * Save Trec Results in a file.
    *
    * @param file output file
    */
  def saveAs(file: String): Unit = {
    val writer = new FileWriter(file)
    for (line <- lines) {
      writer.append(line.toString).append('\n')
    }
    writer.close()
  }
}

object TrecResults extends LazyLogging {
  val TrecSeparator = "\t"
  val TrecSplitter = "\\s+"

  def fromSelected(basename: String, input: String): TrecResults = {
    val features = Features.get(basename)
    val documentIds = Load.selectedDocumentsAt(input)
    val scores = Load.selectedScoresAt(input)
    val titles = features.documentTitles.toIndexedSeq
    val trecIds = features.trecIds

    new TrecResults(
      (for (((qDocIds, qScores), (qTrecId, i)) <- documentIds.zip(scores).zip(trecIds.zipWithIndex)) yield {
        logger.info(s"Processing query $i")
        for (((docId, score), i) <- qDocIds.zip(qScores).zipWithIndex) yield
          new TrecLine(qTrecId, "Q0", titles(docId.toInt), i, score)
      }).flatten
    )
  }

  def fromTrecFile(trecFile: String): TrecResults =
    new TrecResults(
      lines(trecFile)
        .map (line => {
          val s = line.split(TrecSplitter)
          new TrecLine(
            queryId = s(0).toInt,
            topic = s(1),
            title = s(2),
            index =  s(3).toInt,
            score = s(4).toDouble
          )
        })
    )

}