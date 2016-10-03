package edu.nyu.tandon.search.selective

import java.io._

import edu.nyu.tandon.search.selective.ShardSelector.binsWithinBudget
import edu.nyu.tandon.search.selective.data.{Bin, QueryShardExperiment, ShardQueue}
import edu.nyu.tandon.search.selective.data.results._
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */
class ShardSelector(val queryShardExperiment: QueryShardExperiment,
                    val budget: Double)
  extends Iterable[Seq[Int]] {

  /**
    * Get the number of bins to query for each shard.
    */
  def selectedShards(queue: ShardQueue): Seq[Int] = {
    val bins = queue.toList
    val m = binsWithinBudget(bins, budget)
      .groupBy(_.shardId)
      .mapValues(_.length)
      .withDefaultValue(0)
    for (i <- List.range(0, queryShardExperiment.numberOfShards)) yield m(i)
  }

  override def iterator: Iterator[Seq[Int]] = new Iterator[Seq[Int]] {
    val queryDataIterator = queryShardExperiment.iterator
    override def hasNext: Boolean = queryDataIterator.hasNext
    override def next(): Seq[Int] = {
      selectedShards(ShardQueue.maxPayoffQueue(queryDataIterator.next()))
    }
  }

  def selection: Stream[Seq[Int]] = toStream

}

object ShardSelector {

  /**
    * Choose the bins within the budget
    */
  def binsWithinBudget(bins: List[Bin], budget: Double): List[Bin] = {
    val budgets = bins.scanLeft(budget)((budgetLeft, bin) => budgetLeft - bin.cost)
    bins.zip(budgets).zipWithIndex.takeWhile {
      case ((bin: Bin, budget: Double), i: Int) => (budget - bin.cost >= 0 && bin.payoff > 0) || i == 0
    }.unzip._1.unzip._1
  }

  def writeSelection(basename: String, selection: Iterable[Seq[Int]]): Unit = {
    val writer = new BufferedWriter(new FileWriter(s"$basename$SelectionSuffix"))
    for (q <- selection) {
      writer.append(q.mkString(FieldSeparator))
      writer.newLine()
    }
    writer.close()
  }

  def writeSelected(basename: String, selected: Iterable[Seq[Result]]): Unit = {
    val documentsWriter = new BufferedWriter(new FileWriter(s"$basename$SelectedSuffix$ResultsSuffix"))
    for (q <- selected) {
      documentsWriter.append(q.map(_.documentId).mkString(FieldSeparator))
      documentsWriter.newLine()
    }
    documentsWriter.close()
  }

  def writeSelectedScores(basename: String, selected: Iterable[Seq[Result]]): Unit = {
    val scoresWriter = new BufferedWriter(new FileWriter(s"$basename$SelectedSuffix$ScoresSuffix"))
    for (q <- selected) {
      scoresWriter.append(q.map(_.scoreValue).mkString(FieldSeparator))
      scoresWriter.newLine()
    }
    scoresWriter.close()
  }

  def main(args: Array[String]): Unit = {

    case class Config(basename: String = null,
                      budget: Double = 0.0)

    val parser = new OptionParser[Config](this.getClass.getSimpleName) {

      opt[String]('i', "basename")
        .action((x, c) => c.copy(basename = x))
        .text("the prefix of the files")
        .required()

      opt[Double]('b', "budget")
        .action((x, c) => c.copy(budget = x))
        .text("the budget for queries")
        .required()

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        val experiment = QueryShardExperiment.fromBasename(config.basename)
        val selection = new ShardSelector(experiment, config.budget).selection
        writeSelection(config.basename, selection)
        val r = resultsByShardsAndBinsFromBasename(config.basename)
        val selected = r.select(selection)
        writeSelected(config.basename, selected)
        if (r.hasScores) writeSelectedScores(config.basename, selected)

      case None =>
    }

  }

}