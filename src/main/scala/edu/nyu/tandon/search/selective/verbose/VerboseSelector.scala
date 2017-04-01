package edu.nyu.tandon.search.selective.verbose

import java.io.{BufferedWriter, FileNotFoundException, FileWriter}

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
                      val postings: Long = 0,
                      maxTop: Int = 100,
                      scale: Int = 4) {

  def topShards(n: Int): VerboseSelector = {
    val topIds = shards.sortBy(-_.buckets.map(_.impact).sum).take(n).map(_.id).toSet
    new VerboseSelector(
      shards.map(shard =>
        if (topIds.contains(shard.id)) shard
        else Shard(shard.id, List())
      ),
      top,
      lastSelectedShard,
      cost,
      postings,
      maxTop,
      scale
    )
  }

  def selectNext(): Option[VerboseSelector] = {
    val competingBucketsOpt = for (shard <- shards) yield
      if (shard.numSelected < shard.buckets.length) Some(shard.buckets(shard.numSelected))
      else None
    val competingBuckets = competingBucketsOpt.filter(_.nonEmpty).map(_.get)
    if (competingBuckets.isEmpty) None
    else {
      val selected = competingBuckets.maxBy(b => (b.penalty + b.impact) / b.cost)

      /* update queue */
      top.enqueue(selected.results: _*)
      top.enqueue(top.dequeueAll.take(maxTop): _*)

      val selectedShardId = selected.shardId
      Some(
        new VerboseSelector(
          shards.take(selectedShardId)
            ++ Seq(Shard(selectedShardId, shards(selectedShardId).buckets, shards(selectedShardId).numSelected + 1))
            ++ shards.drop(selectedShardId + 1),
          top,
          selectedShardId,
          cost + selected.cost,
          postings + selected.postings
        )
      )
    }
  }

  def round(x: Double): Double = BigDecimal(x).setScale(scale, BigDecimal.RoundingMode.HALF_UP).toDouble

  def precisionAt(k: Int): Double = round(top.clone().dequeueAll.take(k).count(_.relevant).toDouble / k)
  def overlapAt(k: Int): Double = round(top.clone().dequeueAll.take(k).count(_.originalRank <= k).toDouble / k)

  def numRelevantInLastSelected(): Int = {
    assert(lastSelectedShard >= 0 && lastSelectedShard < shards.length, "no last selection to report")
    shards(lastSelectedShard).buckets(lastSelectedBucket).results.count(_.relevant)
  }

  def numTopInLastSelected(k: Int): Int = {
    assert(lastSelectedShard >= 0 && lastSelectedShard < shards.length, "no last selection to report")
    shards(lastSelectedShard).buckets(lastSelectedBucket).results.count(_.originalRank <= k)
  }

  lazy val lastSelectedBucket: Int = shards(lastSelectedShard).numSelected - 1
  lazy val lastSelectedCost: Double = round(shards(lastSelectedShard).buckets(lastSelectedBucket).cost)
  lazy val lastSelectedImpact: Double = round(shards(lastSelectedShard).buckets(lastSelectedBucket).impact)
  lazy val lastSelectedPostings: Long = shards(lastSelectedShard).buckets(lastSelectedBucket).postings
  lazy val totalPostings: Long = shards.map(_.postings).sum
  lazy val postingsRelative: Double = round(postings.toDouble / totalPostings.toDouble)

}

object VerboseSelector extends LazyLogging {

  val CommandName = "verbose-select"

  val scoreOrdering: Ordering[Result] = Ordering.by((result: Result) => result.score)

  def selectors(basename: String, shardPenalty: Double): Seq[VerboseSelector] = {
    val features = Features.get(Properties.get(basename))
    val base = features.baseResults.toList.map(_.map(_.toInt))
    val qrels = try {
      features.qrelsReference
    } catch {
      case e: FileNotFoundException => Seq.fill(base.length)(Seq())
    }
    val data = ZippedIterator(for (shard <- 0 until features.shardCount) yield
      ZippedIterator(for (bucket <- 0 until features.properties.bucketCount) yield {
        val results = Lines.fromFile(Path.toGlobalResults(basename, shard, bucket)).ofSeq[Long].toIndexedSeq
        val scores = Lines.fromFile(Path.toScores(basename, shard, bucket)).ofSeq[Double].toIndexedSeq
        val costs = Lines.fromFile(Path.toCosts(basename, shard, bucket)).of[Double].toIndexedSeq
        val postingCosts = Lines.fromFile(Path.toPostingCosts(basename, shard, bucket)).of[Long].toIndexedSeq
        val impacts = Lines.fromFile(Path.toPayoffs(basename, shard, bucket)).of[Double].toIndexedSeq
        for ((res, bas, qr, score, cost, impact, postingCost) <-
             results.iterator.zip(base.iterator).flatZip(qrels.iterator).flatZip(scores.iterator)
               .flatZip(costs.iterator).flatZip(impacts.iterator).flatZip(postingCosts.iterator)) yield {
          Bucket(shard, (for ((r, s) <- res.zip(score)) yield {
            Result(score = s, relevant = qr.contains(r), originalRank = {
              val idx = bas.indexOf(r)
              if (idx < 0) Int.MaxValue
              else idx
            })
          }).toList, impact, cost, postingCost, penalty = if (bucket == 0) shardPenalty else 0.0)
        }
      }).map(l => new Shard(shard, l.toList)))
    data.map(new VerboseSelector(_)).toIndexedSeq
  }

  def printHeader(precisions: Seq[Int], overlaps: Seq[Int])(writer: BufferedWriter): Unit = {
    writer.write(Seq(
      "qid",
      "step",
      "cost",
      "postings",
      "postings_relative",
      precisions.map(p => s"P@$p").mkString(","),
      overlaps.map(o => s"O@$o").mkString(","),
      "last_shard",
      "last_bucket",
      "last_cost",
      "last_postings",
      "last_impact",
      "last#relevant",
      overlaps.map(o => s"last#top_$o").mkString(",")
    ).mkString(","))
    writer.newLine()
    writer.flush()
  }

  def processSelector(precisions: Seq[Int], overlaps: Seq[Int], maxShards: Int)
                     (qid: Int, selector: VerboseSelector, writer: BufferedWriter): Unit = {

    @tailrec
    def process(selector: VerboseSelector, step: Int = 1): Unit = {

      logger.info(s"Selected [shard=${selector.lastSelectedShard}, bucket=${selector.lastSelectedBucket}, cost=${selector.lastSelectedCost}]")

      writer.write(Seq(
        qid,
        step,
        selector.cost,
        selector.postings,
        selector.postingsRelative,
        precisions.map(selector.precisionAt).mkString(","),
        overlaps.map(selector.overlapAt).mkString(","),
        selector.lastSelectedShard,
        selector.lastSelectedBucket,
        selector.lastSelectedCost,
        selector.lastSelectedPostings,
        selector.lastSelectedImpact,
        selector.numRelevantInLastSelected(),
        overlaps.map(selector.numTopInLastSelected).mkString(",")
      ).mkString(","))

      writer.newLine()

      selector.selectNext() match {
        case Some(nextSelector) => process(nextSelector, step + 1)
        case None =>
      }
    }

    logger.info(s"${selector.shards.map(_.buckets.head.impact)}")
    process(selector.topShards(maxShards).selectNext().get)
    writer.flush()
  }

  def main(args: Array[String]): Unit = {

    case class Config(basename: String = null,
                      precisions: Seq[Int] = Seq(10, 30),
                      overlaps: Seq[Int] = Seq(10, 30),
                      maxShards: Int = Int.MaxValue,
                      shardPenalty: Double = 0.0)

    val parser = new OptionParser[Config](CommandName) {

      arg[String]("<basename>")
        .action((x, c) => c.copy(basename = x))
        .text("the prefix of the files")
        .required()

      opt[Seq[Int]]('p', "precisions")
        .action((x, c) => c.copy(precisions = x))
        .text("k for which to compute P@k")

      opt[Seq[Int]]('o', "overlaps")
        .action((x, c) => c.copy(overlaps = x))
        .text("k for which to compute O@k")

      opt[Double]('P', "penalty")
        .action((x, c) => c.copy(shardPenalty = x))
        .text("shard penalty")

      opt[Int]('m', "max-shards")
        .action((x, c) => c.copy(maxShards = x))
        .text("maximum number of shards to select")

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        logger.info("creating selectors")
        val selectorsForQueries = selectors(config.basename, config.shardPenalty)

        val writer = new BufferedWriter(new FileWriter(s"${config.basename}.verbose"))

        printHeader(config.precisions, config.overlaps)(writer)

        for ((selector, idx) <- selectorsForQueries.zipWithIndex) {
          logger.info(s"processing query $idx")
          logger.debug(s"total number of retrieved relevant documents: ${selector.shards.flatMap(_.buckets).flatMap(_.results).count(_.relevant)}")
          processSelector(config.precisions, config.overlaps, config.maxShards)(idx, selector, writer)
        }

        writer.close()
        logger.info("done")

      case None =>
    }

  }
}

case class Shard(id: Int,
                 buckets: List[Bucket],
                 numSelected: Int = 0) {
  lazy val postings = buckets.map(_.postings).sum
}

case class Bucket(shardId: Int,
                  results: Seq[Result],
                  impact: Double,
                  cost: Double,
                  postings: Long,
                  penalty: Double = 0.0) {
}

case class Result(score: Double,
                  relevant: Boolean,
                  originalRank: Int) {
}