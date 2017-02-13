package edu.nyu.tandon.search.selective.optimize

import java.io.{FileInputStream, FileNotFoundException, ObjectInputStream}

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective._
import edu.nyu.tandon.search.selective.data.Properties
import edu.nyu.tandon.search.selective.data.features.Features
import edu.nyu.tandon.search.selective.optimize.PrecisionOptimizer.Type.Type
import edu.nyu.tandon.utils.Lines._
import edu.nyu.tandon.utils.TupleIterators._
import edu.nyu.tandon.utils.{Lines, ZippedIterator}
import edu.nyu.tandon.utils.WriteLineIterator._
import scopt.OptionParser

import scala.annotation.tailrec
import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.util.Try

/**
  * @author michal.siedlaczek@nyu.edu
  */
object Optimizer {

  def calcPrecision(q: mutable.PriorityQueue[Result], k: Int): Double =
    q.clone().dequeueAll.take(k).map(r => if (r.hit) 1 else 0).sum.toDouble / k

  def takeUntilFirstSatisfiedPrecision(precisions: List[(Double, Double)], targetPrecision: Double): List[(Double, Double)] = precisions match {
    case x :: (y :: s) =>
      if (x._2 >= targetPrecision) List(x)
      else x :: takeUntilFirstSatisfiedPrecision(y :: s, targetPrecision)
    case s => s
  }

}

class PrecisionOptimizer(val shards: List[Shard],
                         val budget: Double,
                         val k: Int,
                         val topResults: mutable.PriorityQueue[Result]
                             = new mutable.PriorityQueue[Result]()(Ordering.by(_.score))) {

  def select(): PrecisionOptimizer = {
    val shardsWithSelected = shards.map(shard => shard.selectWithBudget(topResults, k, budget))
    val (selected, maxIdx) = shardsWithSelected.zipWithIndex
      .maxBy { case (s, idx) => s match {
        case Some((shard, precision, cost)) => precision / cost
        case None => -1.0
      }}
    selected match {
      case Some((selectedShard, _, selectedCost)) =>
        val prefixWithDiscarded = shards.take(maxIdx + 1)
        for (b <- selectedShard.buckets.slice(prefixWithDiscarded.last.numSelected, selectedShard.numSelected))
          topResults.enqueue(b.results:_*)
        topResults.enqueue(topResults.dequeueAll.take(k):_*)
        val newShardList = prefixWithDiscarded.dropRight(1) ++ List(selectedShard) ++ shards.drop(maxIdx + 1)
        new PrecisionOptimizer(newShardList, budget - selectedCost, k, topResults)
      case None =>
        new PrecisionOptimizer(shards, 0, k, topResults)
    }
  }

  @tailrec
  final def optimize(): PrecisionOptimizer = {
    val selected = select()
    if (selected.budget == 0.0) selected
    else selected.optimize()
  }

}

object PrecisionOptimizer extends LazyLogging {

  val CommandName = "optimize-precision"

  object Type extends Enumeration {
    type Type = Value
    val relevance, overlap = Value
  }

  implicit val typeRead: scopt.Read[Type.Value] =
    scopt.Read.reads(Type.withName)

  def overlapOptimizers(basename: String, budget: Double, k: Int) = {
    val features = Features.get(Properties.get(basename))
    val reference = features.baseResults.toList

    ZippedIterator(for (shard <- 0 until features.shardCount) yield
      ZippedIterator(for (bucket <- 0 until features.properties.bucketCount) yield {
        val results = Lines.fromFile(Path.toGlobalResults(basename, shard, bucket)).ofSeq[Long]
        val scores = Lines.fromFile(Path.toScores(basename, shard, bucket)).ofSeq[Double]
        val costs = Lines.fromFile(Path.toCosts(basename, shard, bucket)).of[Double]
        for ((res, ref, score, cost) <- results.zip(reference.iterator).flatZip(scores).flatZip(costs)) yield {
          Bucket((for ((r, s) <- res.zip(score)) yield {
            Result(s, ref.contains(r))
          }).toList, cost)
        }
      }).map(l => new Shard(l.toList))).map(s => new PrecisionOptimizer(s.toList, budget, k))
  }

  def relevanceOptimizers(basename: String, budget: Double, k: Int) = {
    val features = Features.get(Properties.get(basename))
    val reference = features.qrelsReference

    ZippedIterator(for (shard <- 0 until features.shardCount) yield
      ZippedIterator(for (bucket <- 0 until features.properties.bucketCount) yield {
        val results = Lines.fromFile(Path.toGlobalResults(basename, shard, bucket)).ofSeq[Long]
        val scores = Lines.fromFile(Path.toScores(basename, shard, bucket)).ofSeq[Double]
        val costs = Lines.fromFile(Path.toCosts(basename, shard, bucket)).of[Double]
        for ((res, ref, score, cost) <- results.zip(reference.iterator).flatZip(scores).flatZip(costs)) yield {
          Bucket((for ((r, s) <- res.zip(score)) yield {
            Result(s, ref.contains(r))
          }).toList, cost)
        }
      }).map(l => new Shard(l.toList))).map(s => new PrecisionOptimizer(s.toList, budget, k))
  }

  def main(args: Array[String]): Unit = {

    case class Config(basename: String = null,
                      budget: Double = 0.0,
                      k: Int = 0,
                      operation: Type = null)

    val parser = new OptionParser[Config](CommandName) {

      arg[Type]("<type>")
        .action((x, c) => c.copy(operation = x))
        .required()

      arg[String]("<basename>")
        .action((x, c) => c.copy(basename = x))
        .text("the prefix of the files")
        .required()

      opt[Double]('b', "budget")
        .action((x, c) => c.copy(budget = x))
        .text("the budget for queries")
        .required()

      opt[Int]('k', "k")
        .action((x, c) => c.copy(k = x))
        .text("the number of top results to optimize")
        .required()

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        val optimizers = config.operation match {
          case Type.overlap => overlapOptimizers(config.basename, config.budget, config.k)
          case Type.relevance => relevanceOptimizers(config.basename, config.budget, config.k)
        }

        val optimized = (for (optimizer <- optimizers) yield optimizer.optimize().shards.map(_.numSelected)).toStream
        ShardSelector.writeSelection(optimized, s"${config.basename}$BudgetIndicator[opt]")

      case None =>
    }

  }

}

class BudgetOptimizer(val shards: List[Shard],
                      val targetPrecision: Double,
                      val k: Int,
                      val topResults: mutable.PriorityQueue[Result]
                      = new mutable.PriorityQueue[Result]()(Ordering.by(_.score))) {

  def select(): BudgetOptimizer = {
    val shardsWithSelected = shards.map(shard => shard.selectWithPrecision(topResults, k, targetPrecision))
    val (selected, maxIdx) = shardsWithSelected.zipWithIndex
      .maxBy { case (s, idx) => s match {
        case Some((shard, precision, cost)) => precision / cost
        case None => -1.0
      }}
    selected match {
      case Some((selectedShard, _, selectedCost)) =>
        val prefixWithDiscarded = shards.take(maxIdx + 1)
        for (b <- selectedShard.buckets.slice(prefixWithDiscarded.last.numSelected, selectedShard.numSelected))
          topResults.enqueue(b.results:_*)
        topResults.enqueue(topResults.dequeueAll.take(k):_*)
        val newShardList = prefixWithDiscarded.dropRight(1) ++ List(selectedShard) ++ shards.drop(maxIdx + 1)
        new BudgetOptimizer(newShardList, targetPrecision, k, topResults)
      case None =>
        new BudgetOptimizer(shards, 0, k, topResults)
    }
  }

  @tailrec
  final def optimize(): BudgetOptimizer = {
    val selected = select()
    if (Optimizer.calcPrecision(selected.topResults, k) >= targetPrecision) selected
    else selected.optimize()
  }

}

class Shard(val buckets: List[Bucket],
            val numSelected: Int = 0) {

  val numBuckets = buckets.length

  def calcPrecisions(q: mutable.PriorityQueue[Result], k: Int) =
    buckets
      .drop(numSelected)
      .scanLeft((0.0, 0.0))((acc, bucket) => {
        q.enqueue(bucket.results:_*)
        q.enqueue(q.dequeueAll.take(k):_*)
        (acc._1 + bucket.cost, Optimizer.calcPrecision(q, k))
      }).drop(1)

  def selectWithBudget(topResults: mutable.PriorityQueue[Result], k: Int, budget: Double): Option[(Shard, Double, Double)] = {
    val initialPrecision = Optimizer.calcPrecision(topResults, k)
    val q = topResults.clone()
    val precisions = calcPrecisions(q, k).takeWhile { case (c, _) => c <= budget }
    if (precisions.isEmpty) None
    else {
      val filtered = precisions.zipWithIndex.filter { case ((c, p), i) => p > initialPrecision }
      if (filtered.isEmpty) None
      else {
        val ((cost, precision), idx) = filtered.maxBy { case ((c, p), _) => p / c }
        Some(new Shard(buckets, numSelected + idx + 1), precision, cost)
      }
    }
  }

  def selectWithPrecision(topResults: mutable.PriorityQueue[Result], k: Int, targetPrecision: Double): Option[(Shard, Double, Double)] = {
    val q = topResults.clone()
    val precisions = Optimizer.takeUntilFirstSatisfiedPrecision(calcPrecisions(q, k), targetPrecision)
    if (precisions.isEmpty) None
    else {
      val ((cost, precision), idx) = precisions.zipWithIndex.maxBy { case ((c, p), _) => p / c }
      Some(new Shard(buckets, numSelected + idx + 1), precision, cost)
    }
  }

}

case class Bucket(results: List[Result],
                  cost: Double) {
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