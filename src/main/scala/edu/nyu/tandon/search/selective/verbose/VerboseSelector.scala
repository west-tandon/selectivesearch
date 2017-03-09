package edu.nyu.tandon.search.selective.verbose

import java.io.BufferedWriter

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective.Path
import edu.nyu.tandon.search.selective.data.Properties
import edu.nyu.tandon.search.selective.data.features.Features
import edu.nyu.tandon.search.selective.verbose.VerboseSelector.scoreOrdering
import edu.nyu.tandon.utils.Lines._
import edu.nyu.tandon.utils.TupleIterators._
import edu.nyu.tandon.utils.{Lines, ZippedIterator}
import scopt.OptionParser

import scala.annotation.tailrec
import scala.collection.mutable

/**
  * @author michal.siedlaczek@nyu.edu
  */
class VerboseSelector(val shards: Seq[Shard],
                      top: mutable.PriorityQueue[Result] = new mutable.PriorityQueue[Result]()(scoreOrdering),
                      val lastSelectedShard: Int = -1,
                      val cost: Double = 0,
                      maxTop: Int = 100,
                      scale: Int = 4) {

  def selectNext(): Option[VerboseSelector] = {
    val competingBucketsOpt = for (shard <- shards) yield
      if (shard.numSelected < shard.buckets.length) Some(shard.buckets(shard.numSelected))
      else None
    val competingBuckets = competingBucketsOpt.filter(_.nonEmpty).map(_.get)
    if (competingBuckets.isEmpty) None
    else {
      val selected = competingBuckets.maxBy(b => b.impact / b.cost)

      /* update queue */
      top.enqueue(selected.results: _*)
      top.enqueue(top.dequeueAll.take(maxTop): _*)

      val selectedShardId = selected.shardId
      Some(new VerboseSelector(shards.take(selectedShardId)
        ++ Seq(Shard(shards(selectedShardId).buckets, shards(selectedShardId).numSelected + 1))
        ++ shards.drop(selectedShardId + 1),
        top, selectedShardId, cost + selected.cost))
    }
  }

  def round(x: Double): Double = BigDecimal(x).setScale(scale, BigDecimal.RoundingMode.HALF_UP).toDouble

  def precisionAt(k: Int): Double = round(top.clone().dequeueAll.take(k).count(_.relevant).toDouble / k)
  def overlapAt(k: Int): Double = round(top.clone().dequeueAll.take(k).count(_.originalRank <= k).toDouble / k)

  def numRelevantInLastSelected(): Int = {
    assert(lastSelectedShard >= 0 && lastSelectedShard < shards.length, "no last selection to report")
    shards(lastSelectedShard).buckets.take(lastSelectedBucket + 1).flatMap(_.results).count(_.relevant)
  }

  def numTopInLastSelected(k: Int): Int = {
    assert(lastSelectedShard >= 0 && lastSelectedShard < shards.length, "no last selection to report")
    shards(lastSelectedShard).buckets.take(lastSelectedBucket + 1).flatMap(_.results).count(_.originalRank <= k)
  }

  lazy val lastSelectedBucket: Int = shards(lastSelectedShard).numSelected - 1
  lazy val lastSelectedCost: Double = shards(lastSelectedShard).buckets(lastSelectedBucket).cost

}

object VerboseSelector extends LazyLogging {

  val CommandName = "verbose-select"

  val scoreOrdering: Ordering[Result] = Ordering.by((result: Result) => result.score)

  def selectors(basename: String): Seq[VerboseSelector] = {
    val features = Features.get(Properties.get(basename))
    val base = features.baseResults.toList.map(_.map(_.toInt))
    val qrels = features.qrelsReference
    val data = ZippedIterator(for (shard <- 0 until features.shardCount) yield
      ZippedIterator(for (bucket <- 0 until features.properties.bucketCount) yield {
        val results = Lines.fromFile(Path.toGlobalResults(basename, shard, bucket)).ofSeq[Long]
        val scores = Lines.fromFile(Path.toScores(basename, shard, bucket)).ofSeq[Double]
        val costs = Lines.fromFile(Path.toPostingCosts(basename, shard, bucket)).of[Double]
        val impacts = Lines.fromFile(Path.toPayoffs(basename, shard, bucket)).of[Double]
        for ((res, bas, qrels, score, cost, impact) <-
             results.zip(base.iterator).flatZip(qrels.iterator).flatZip(scores).flatZip(costs).flatZip(impacts)) yield {
          Bucket(shard, (for ((r, s) <- res.zip(score)) yield {
            Result(score = s, relevant = qrels.contains(r), originalRank = {
              val idx = bas.indexOf(r)
              if (idx < 0) Int.MaxValue
              else idx
            })
          }).toList, impact, cost)
        }
      }).map(l => new Shard(l.toList)))
    data.map(new VerboseSelector(_)).toSeq
  }

  def printHeader(precisions: Seq[Int], overlaps: Seq[Int])(writer: BufferedWriter): Unit = {
    writer.write(Seq(
      "qid",
      "step",
      "cost",
      precisions.map(p => s"P@$p").mkString(","),
      overlaps.map(o => s"O@$o").mkString(","),
      "last_shard",
      "last_bucket",
      "last_cost",
      "last#relevant",
      overlaps.map(o => s"last#top_$o").mkString(",")
    ).mkString(","))
    writer.newLine()
    writer.flush()
  }

  def processSelector(precisions: Seq[Int], overlaps: Seq[Int])
                     (qid: Int, selector: VerboseSelector, writer: BufferedWriter): Unit = {

    @tailrec
    def process(selector: VerboseSelector, step: Int = 0): Unit = {

      writer.write(Seq(
        qid,
        step,
        selector.cost,
        precisions.map(selector.precisionAt).mkString(","),
        overlaps.map(selector.overlapAt).mkString(","),
        selector.lastSelectedShard,
        selector.lastSelectedBucket,
        selector.lastSelectedCost,
        selector.numRelevantInLastSelected(),
        overlaps.map(selector.numTopInLastSelected).mkString(",")
      ).mkString(","))

      writer.newLine()

      selector.selectNext() match {
        case Some(nextSelector) => process(nextSelector, step + 1)
        case None =>
      }
    }

    process(selector.selectNext().get)
    writer.flush()
  }

  def main(args: Array[String]): Unit = {

    case class Config(basename: String = null)

    val parser = new OptionParser[Config](CommandName) {

      arg[String]("<basename>")
        .action((x, c) => c.copy(basename = x))
        .text("the prefix of the files")
        .required()

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        logger.info("creating selectors")
        val selectorsForQueries = selectors(config.basename)
//        val budgetBasename = s"${config.basename}$BudgetIndicator[${config.budgetStr}]"
//
        for ((selector, idx) <- selectorsForQueries.zipWithIndex) {
          logger.info(s"processing query $idx")
//          processSelector(idx, selector)
        }
//        val selection = (for ((selector, idx) <- selectorsForQueries.zipWithIndex)
//          yield {
//            logger.info(s"selection for query $idx")
//            selector.select(selector.threshold).shards.map(_.numSelected)
//          }).toStream
//        ShardSelector.writeSelection(selection, budgetBasename)
//        val selected = data.results.resultsByShardsAndBucketsFromBasename(config.basename)
//          .select(selection).toSeq
//        ShardSelector.writeSelected(selected, budgetBasename)

      case None =>
    }

  }
}

case class Shard(buckets: List[Bucket],
                 numSelected: Int = 0) {

}

case class Bucket(shardId: Int,
                  results: Seq[Result],
                  impact: Double,
                  cost: Double) {
}

case class Result(score: Double,
                  relevant: Boolean,
                  originalRank: Int) {
}