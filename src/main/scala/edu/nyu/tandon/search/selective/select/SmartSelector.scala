package edu.nyu.tandon.search.selective.select

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon._
import edu.nyu.tandon.search.selective.data.Properties
import edu.nyu.tandon.search.selective.data.features.Features
import edu.nyu.tandon.search.selective.{ShardSelector, data, _}
import edu.nyu.tandon.utils.Lines._
import edu.nyu.tandon.utils.{Lines, ZippedIterator}
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */
case class SmartSelector(shards: List[Shard],
                         budget: Double,
                         k: Int) {

  assert(k > 0, "k must be > 0")

  def select(threshold: Double = 0.0): SmartSelector = {
    val (withRemainingShards, withRemainingBuckets) = shards match {
      case Nil => (None, None)
      case shard :: tail =>
        val wrs = tail match {
          case Nil => None
          case _ =>
            val remainingShards = SmartSelector(tail, budget - shard.costOfSelected, k).select()
            Some(SmartSelector(shard :: remainingShards.shards, remainingShards.budget, k))
        }
        val wrb = shard.nextAvailable(threshold) match {
          case None => None
          case Some(s) =>
            if (budget - s.costOfSelected < 0) None
            else Some(SmartSelector(s :: tail, budget, k).select())
        }
        (wrs, wrb)
    }
    Array(withRemainingBuckets, withRemainingShards, Some(this)).sortBy{
      case None => -1.0
      case Some(c) => c.impact
    }.last.get
  }

  def impact: Double = shards.flatMap(s => s.buckets.take(s.numSelected).map(_.impact)).sum

}

object SmartSelector extends LazyLogging {

  val CommandName = "select"

  def selectors(basename: String, budget: Double, k: Int): List[SmartSelector] = {
    val features = Features.get(Properties.get(basename))

    val data = ZippedIterator(for (shard <- 0 until features.shardCount) yield
      ZippedIterator(for (bucket <- 0 until features.properties.bucketCount) yield {
        val payoffs = Lines.fromFile(Path.toPayoffs(basename, shard, bucket)).of[Long]
        val costs = Lines.fromFile(Path.toCosts(basename, shard, bucket)).of[Double]
        for ((pay, cost) <- payoffs.zip(costs)) yield {
          Bucket(pay, cost)
        }
      }).zipWithIndex.map{ case (l, id) => Shard(l.toList)})

    data.map(s => SmartSelector(s.toList, budget, k)).toList
  }

  def main(args: Array[String]): Unit = {

    case class Config(basename: String = null,
                      budget: Double = 0.0,
                      budgetStr: String = null,
                      k: Int = 0)

    val parser = new OptionParser[Config](CommandName) {

      arg[String]("<basename>")
        .action((x, c) => c.copy(basename = x))
        .text("the prefix of the files")
        .required()

      opt[String]('b', "budget")
        .action((x, c) => c.copy(budgetStr = x, budget = doubleConverter(x)))
        .text("the budget for queries")
        .required()

      opt[Int]('k', "k")
        .action((x, c) => c.copy(k = x))
        .text("the number of top results to optimize")
        .required()

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        logger.info("creating selectors")
        val selectorsForQueries = selectors(config.basename, config.budget, config.k)
        val budgetBasename = s"${config.basename}$BudgetIndicator[${config.budgetStr}]"

        val selection = (for ((selector, idx) <- selectorsForQueries.zipWithIndex)
          yield {
            logger.info(s"selection for query $idx")
            val threshold = selector.shards.toArray.flatMap(_.buckets.map(_.impact))
              .sorted(Ordering[Double].reverse)
              .take(config.k)
              .last
            selector.select(threshold).shards.map(_.numSelected)
          }).toStream
        ShardSelector.writeSelection(selection, budgetBasename)
        val selected = data.results.resultsByShardsAndBucketsFromBasename(config.basename)
          .select(selection).toSeq
        ShardSelector.writeSelected(selected, budgetBasename)

      case None =>
    }

  }

}

case class Shard(buckets: List[Bucket],
                 numSelected: Int = 0,
                 costOfSelected: Double = 0.0) {

  val numBuckets = buckets.length

  def nextAvailable(threshold: Double = 0.0): Option[Shard] = {
    val taken = takeUntilOrNil(buckets.drop(numSelected))(_.impact > threshold)
    if (taken == Nil) None
    else {
      val (selected, cost) = taken.foldLeft((numSelected, costOfSelected)) {
        case ((selectedAcc, costAcc), bucket) => (selectedAcc + 1, costAcc + bucket.cost)
      }
      Some(Shard(buckets, selected, cost))
    }
  }

}

case class Bucket(impact: Double,
                  cost: Double) {
}