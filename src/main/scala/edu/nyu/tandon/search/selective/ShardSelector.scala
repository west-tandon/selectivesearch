package edu.nyu.tandon.search.selective

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective.ShardSelector.bucketsWithinBudget
import edu.nyu.tandon.search.selective.data.results._
import edu.nyu.tandon.search.selective.data.{Bucket, QueryShardExperiment, ShardQueue}
import edu.nyu.tandon.utils.WriteLineIterator._
import scopt.OptionParser

import scalax.io.StandardOpenOption._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class ShardSelector(val queryShardExperiment: QueryShardExperiment,
                    val budget: Double)
  extends Iterable[Seq[Int]] {

  /**
    * Get the number of buckets to query for each shard.
    */
  def selectedShards(queue: ShardQueue): Seq[Int] = {
    val buckets = queue.toList
    val m = bucketsWithinBudget(buckets, budget)
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
  def bucketsWithinBudget(buckets: List[Bucket], budget: Double): List[Bucket] = {

    var remainingBudget = budget
    val budgets = for (bucket <- buckets) yield {
      val value = remainingBudget - bucket.cost
      if (value >= 0) remainingBudget = value
      value
    }

    budgets match {
      case head :: tail =>
        if (head < 0) return List(buckets.head)
        buckets.zip(budgets)
          .filter(_._2 >= 0)
          .unzip._1
      case _ => List()
    }
  }

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
                      budget: Double = 0.0)

    val parser = new OptionParser[Config](CommandName) {

      arg[String]("<basename>")
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

        logger.info(s"Selecting shards from ${config.basename} with budget ${config.budget}")

        val budgetBasename = s"${config.basename}$BudgetIndicator[${config.budget}]"

        val experiment = QueryShardExperiment.fromBasename(config.basename)

        val selection = new ShardSelector(experiment, config.budget).selection
        writeSelection(selection, budgetBasename)

        val selected = resultsByShardsAndBucketsFromBasename(config.basename)
          .select(selection).toSeq
        writeSelected(selected, budgetBasename)

      case None =>
    }

  }

}