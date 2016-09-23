package edu.nyu.tandon.search.selective

import java.io.Writer

import edu.nyu.tandon.search.selective.data.{Bin, QueryData, QueryExperiment, ShardQueue}
import scopt.OptionParser

/**
  *
  * Files:
  *   *.payoff
  *   *.cost
  *     - each line is a QUERY; each field is a BIN
  *   *.division
  *     - each field is a lenght (in terms of bins) of an i-th SHARD
  *   *.selection
  *     - each line is a QUERY; each field is the number of chosen bins in a SHARD
  *
  * @author michal.siedlaczek@nyu.edu
  */
object SelectBins {

  /**
    * Get the number of bins to query for each shard.
    */
  def shards(queue: ShardQueue, budget: Double): Map[Int, Int] = {
    val bins = queue.toList
    binsWithinBudget(bins, budget)
      .groupBy(_.shardId)
      .mapValues(_.length)
      .withDefaultValue(0)
  }

  /**
    * Choose the bins within the budget
    */
  def binsWithinBudget(bins: Seq[Bin], budget: Double): Seq[Bin] = {
    val budgets = bins.scanLeft(budget)((budgetLeft, bin) => budgetLeft - bin.cost)
    (bins zip budgets).takeWhile {
      case (bin: Bin, budget: Double) => budget > 0
    }.unzip._1
  }

  def writeQueryChoices(numShards: Int, choices: Map[Int, Int], writer: Writer): Unit = {
    val choiceList = for (i <- 0 until numShards) yield choices(i)
    writer.append(choiceList.mkString(","))
  }

  def select(queryData: QueryData) = {
    val shardQueue = ShardQueue.maxPayoffQueue(queryData)
    ???
  }

  def main(args: Array[String]): Unit = {

    case class Config(basename: String = null)

    val parser = new OptionParser[Config](this.getClass.getSimpleName) {

      opt[String]('b', "basename")
        .action((x, c) => c.copy(basename = x))
        .text("the prefix of the files")
        .required()

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        val experiment = new QueryExperiment(config.basename)

      case None =>
    }

  }

}
