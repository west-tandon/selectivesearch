package edu.nyu.tandon.search.selective

import java.io._

import edu.nyu.tandon.search.selective.ShardSelector.binsWithinBudget
import edu.nyu.tandon.search.selective.data.{Bin, QueryData, QueryExperiment, ShardQueue}
import scopt.OptionParser

/**
  *
  * Files:
  *   *.payoff
  *   *.cost
  *     - each line is a QUERY; each field is a BIN
  *   *.division
  *     - each field is a length (in terms of bins) of an i-th SHARD
  *   *.selection
  *     - each line is a QUERY; each field is the number of chosen bins in a SHARD
  *
  * @author michal.siedlaczek@nyu.edu
  */
class ShardSelector(val queryDataIterator: Iterator[QueryData],
                    val shardIds: List[Int],
                    val budget: Double)
  extends Iterable[List[Int]] {

  /**
    * Get the number of bins to query for each shard.
    */
  def selectedShards(queue: ShardQueue): List[Int] = {
    val bins = queue.toList
    val m = binsWithinBudget(bins, budget)
      .groupBy(_.shardId)
      .mapValues(_.length)
      .withDefaultValue(0)
    for (i <- shardIds.distinct) yield m(i)
  }

  override def iterator: Iterator[List[Int]] = new Iterator[List[Int]] {
    override def hasNext: Boolean = queryDataIterator.hasNext
    override def next(): List[Int] = {
      selectedShards(ShardQueue.maxPayoffQueue(queryDataIterator.next()))
    }
  }

  def write(outputFile: String): Unit =
    write(new FileOutputStream(outputFile))

  def write(outputStream: OutputStream): Unit = {
    val writer = new BufferedWriter(new OutputStreamWriter(outputStream))
    for (q <- this) {
      writer.append(q.mkString(QueryExperiment.FieldSeparator))
      writer.newLine()
    }
    writer.close()
  }

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

  def main(args: Array[String]): Unit = {

    case class Config(basename: String = null,
                      budget: Double = 0.0)

    val parser = new OptionParser[Config](this.getClass.getSimpleName) {

      opt[String]('n', "basename")
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

        val experiment = new QueryExperiment(config.basename)
        val shardSelector = new ShardSelector(experiment.iterator, experiment.shardIds, config.budget)
        shardSelector.write(config.basename + QueryExperiment.SelectionSuffix)

      case None =>
    }

  }

}