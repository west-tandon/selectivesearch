package edu.nyu.tandon.search.selective

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective.data.results._
import edu.nyu.tandon.search.selective.data.{Bucket, QueryShardExperiment, ShardQueue}
import edu.nyu.tandon.utils.WriteLineIterator._
import scopt.OptionParser

import scalax.io.StandardOpenOption._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class ShardSelector(val queryShardExperiment: QueryShardExperiment,
                    val selectionStrategy: List[Bucket] => List[Bucket])
  extends Iterable[Seq[Int]] {

  /**
    * Get the number of buckets to query for each shard.
    */
  def selectedShards(queue: ShardQueue): Seq[Int] = {
    val m = selectionStrategy(queue.toList)
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

object ShardSelector extends LazyLogging {

  val CommandName = "select-shards"

  /**
    * Choose the buckets within the budget
    */
  def bucketsWithinBudget(budget: Double)(buckets: List[Bucket]): List[Bucket] = {
    val budgets = buckets.scanLeft(budget)((budgetLeft, bucket) => budgetLeft - bucket.cost)
    buckets.zip(budgets).zipWithIndex.takeWhile {
      case ((bucket: Bucket, budget: Double), i: Int) => budget - bucket.cost >= 0 || i == 0
    }.unzip._1.unzip._1
  }

  def bucketsUntilThreshold(threshold: Double)(buckets: List[Bucket]): List[Bucket] =
    buckets.takeWhile(_.payoff > threshold)

  def writeSelection(selection: Stream[Seq[Int]], basename: String): Unit = {
    selection.map(_.mkString(FieldSeparator)).write(Path.toSelection(basename))
    val (sum, count) = selection.map(_.count(_ > 0)).aggregating.write(Path.toSelectionShardCount(basename))
    scalax.file.Path.fromString(s"${Path.toSelectionShardCount(basename)}.avg").outputStream(WriteTruncate:_*)
      .write(String.valueOf(sum / count.toDouble))
  }

  def writeSelected(selected: Seq[Seq[Result]], basename: String): Unit = {
    selected.map(_.map(_.globalDocumentId).mkString(FieldSeparator)).write(Path.toSelectedDocuments(basename))
    selected.map(_.map(_.score).mkString(FieldSeparator)).write(Path.toSelectedScores(basename))
  }

  def main(args: Array[String]): Unit = {

    case class Config(basename: String = null,
                      budget: Double = 0.0,
                      budgetDefined: Boolean = false,
                      threshold: Double = 0.0,
                      thresholdDefined: Boolean = false,
                      paramStr: String = null)

    val parser = new OptionParser[Config](CommandName) {

      arg[String]("<basename>")
        .action((x, c) => c.copy(basename = x))
        .text("the prefix of the files")
        .required()

      opt[String]('b', "budget")
        .action((x, c) => c.copy(paramStr = x, budget = x.toDouble, budgetDefined = true))
        .text("the budget for queries")

      opt[String]('t', "threshold")
        .action((x, c) => c.copy(paramStr = x, threshold = x.toDouble, thresholdDefined = true))
        .text("the threshold for bucket payoffs")

      checkConfig(c =>
        if ((!c.budgetDefined && !c.thresholdDefined) || (c.budgetDefined && c.thresholdDefined))
          failure("define either budget or threshold")
        else success
      )

      checkConfig(c =>
        if (c.budgetDefined && c.budget <= 0) failure("budget must be positive")
        else success
      )

      checkConfig(c =>
        if (c.thresholdDefined && c.threshold < 0) failure("threshold must be non-negative")
        else success
      )

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        logger.info(s"Selecting shards from ${config.basename} with ${if (config.budgetDefined) "budget" else "threshold"} ${config.paramStr}")

        val indicator = if (config.budgetDefined) BudgetIndicator else ThresholdIndicator
        val budgetBasename = s"${config.basename}$indicator[${config.paramStr}]"

        val experiment = QueryShardExperiment.fromBasename(config.basename)

        val strategy: List[Bucket] => List[Bucket] = if (config.budgetDefined) bucketsWithinBudget(config.budget)
        else bucketsUntilThreshold(config.threshold)
        val selection = new ShardSelector(experiment, strategy).selection
        writeSelection(selection, budgetBasename)

        val selected = data.results.resultsByShardsAndBucketsFromBasename(config.basename)
          .select(selection).toSeq
        writeSelected(selected, budgetBasename)

      case None =>
    }

  }

}