package edu.nyu.tandon.search.selective.clairvoyant

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective.{ShardSelector, data, _}
import edu.nyu.tandon.search.selective.data.Properties
import edu.nyu.tandon.search.selective.data.features.Features
import edu.nyu.tandon.takeUntilOrNil
import edu.nyu.tandon.utils.{Lines, ZippedIterator}
import edu.nyu.tandon.utils.Lines._
import edu.nyu.tandon.utils.TupleIterators._
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */
case class ClairvoyantSelector(shards: List[Shard],
                          budget: Double,
                          k: Int) extends LazyLogging {

  assert(k > 0, "k must be > 0")

  lazy val selected = {
    val sel = ClairvoyantSelector(
      shards.filter(_.buckets.flatMap(_.results).exists(_.hit)),
      budget, k).select()
    val selectedMap = sel.shards.map(s => (s.id, s)).toMap
    val originalMap = shards.map(s => (s.id, s)).toMap
    ClairvoyantSelector((for (id <- shards.indices) yield selectedMap.withDefault(originalMap)(id)).toList, budget, k)
  }

  def select(): ClairvoyantSelector = {
    logger.debug(s"select: shards.head.numSelected=${shards.head.numSelected}, shards.tail.length=${shards.tail.length}")
    val (withRemainingShards, withRemainingBuckets) = shards match {
      case Nil => (None, None)
      case shard :: tail =>
        val wrs = tail match {
          case Nil => None
          case _ =>
            val remainingShards = ClairvoyantSelector(tail, budget - shard.costOfSelected, k).select()
            Some(ClairvoyantSelector(shard :: remainingShards.shards, remainingShards.budget, k))
        }
        val wrb = shard.nextAvailable match {
          case None => None
          case Some(s) =>
            if (budget - s.costOfSelected < 0) None
            else Some(ClairvoyantSelector(s :: tail, budget, k).select())
        }
        (wrs, wrb)
    }
    Array(withRemainingBuckets, withRemainingShards, Some(this)).sortBy{
      case None => -1.0
      case Some(c) => c.overlap
    }.last.get
  }

  def overlap: Double = {
    val selectedResults = shards.flatMap(s => s.buckets.take(s.numSelected).flatMap(_.results))
    val hitCount = selectedResults
      .sortBy(_.score)(Ordering[Double].reverse)
      .take(k)
      .count(_.hit)
    hitCount.toDouble / k
  }

}

object ClairvoyantSelector extends LazyLogging {

  val CommandName = "select-opt"

  def selectors(basename: String, budget: Double, k: Int): List[ClairvoyantSelector] = {
    val features = Features.get(Properties.get(basename))
    val reference = features.baseResults.toList.map(_.map(_.toInt))

    val data = ZippedIterator(for (shard <- 0 until features.shardCount) yield
      ZippedIterator(for (bucket <- 0 until features.properties.bucketCount) yield {
        val results = Lines.fromFile(Path.toGlobalResults(basename, shard, bucket)).ofSeq[Long]
        val scores = Lines.fromFile(Path.toScores(basename, shard, bucket)).ofSeq[Double]
        val costs = Lines.fromFile(Path.toCosts(basename, shard, bucket)).of[Double]
        for ((res, ref, score, cost) <- results.zip(reference.iterator).flatZip(scores).flatZip(costs)) yield {
          Bucket((for ((r, s) <- res.zip(score)) yield {
            Result(s, ref.take(k).contains(r))
          }).toList, cost)
        }
      }).zipWithIndex.map{ case (l, id) => Shard(id, l.toList)})

    data.map(s => ClairvoyantSelector(s.toList, budget, k)).toList
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
            selector.select().shards.map(_.numSelected)
          }).toStream
        ShardSelector.writeSelection(selection, budgetBasename)
        val selected = data.results.resultsByShardsAndBucketsFromBasename(config.basename)
          .select(selection).toSeq
        ShardSelector.writeSelected(selected, budgetBasename)

      case None =>
    }

  }

}

case class Shard(id: Int,
                 buckets: List[Bucket],
                 numSelected: Int = 0,
                 costOfSelected: Double = 0.0) {

  val numBuckets = buckets.length

  def nextAvailable: Option[Shard] = {
    val taken = takeUntilOrNil(buckets.drop(numSelected))(_.numHits > 0)
    if (taken == Nil) None
    else {
      val (selected, cost) = taken.foldLeft((numSelected, costOfSelected)) {
        case ((selectedAcc, costAcc), bucket) => (selectedAcc + 1, costAcc + bucket.cost)
      }
      Some(Shard(id, buckets, selected, cost))
    }
  }

}

case class Bucket(results: List[Result],
                  cost: Double) {
  val numResults = results.length
  lazy val numHits = results.count(_.hit)
}

/**
  *
  * @param score  score of the document
  * @param hit    whether the result is counted as positive hit in precision
  *               (e.g., labeled relevant or in top-k of exhaustive search)
  */
case class Result(score: Double,
                  hit: Boolean) {
}