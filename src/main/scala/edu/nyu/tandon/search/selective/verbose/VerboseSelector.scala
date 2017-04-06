package edu.nyu.tandon.search.selective.verbose

import java.io.{BufferedWriter, File, FileWriter}

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective.data.Properties
import edu.nyu.tandon.search.selective.data.features.Features
import edu.nyu.tandon.search.selective.verbose.VerboseSelector.scoreOrdering
import org.apache.spark.sql.{Row, SparkSession}
import scopt.OptionParser
import org.apache.spark.sql.functions.{sum, when}

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
                      maxTop: Int = 500,
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
  def complexRecall(k: Int): Double = round(top.clone().dequeueAll.count(_.complexRank <= k).toDouble / k)

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

  def selectors(basename: String, shardPenalty: Double): Iterator[VerboseSelector] = {
    val properties = Properties.get(basename)
    val features = Features.get(properties)
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val shardResults = for (shard <- 0 until properties.shardCount) yield
      spark.read.parquet(s"${features.basename}#$shard.results-${properties.bucketCount}")
    val costs =
      if (new File(s"basename#0.cost").exists())
        Some(for (shard <- 0 until properties.shardCount) yield
          spark.read.parquet(s"$basename#$shard.cost"))
      else None
    val postingCosts = for (shard <- 0 until properties.shardCount) yield
      spark.read.parquet(s"${features.basename}#$shard.postingcost-${properties.bucketCount}")
    val impacts = for (shard <- 0 until properties.shardCount) yield
      spark.read.parquet(s"$basename#$shard.impacts")
    val baseResults = spark.read.parquet(s"${features.basename}.results")
    val queryFeatures = spark.read.parquet(s"${features.basename}.queryfeatures")

    queryFeatures.select("query")
      .orderBy("query")
      .collect()
      .toIterator
      .map {
        case Row(queryId) =>
          logger.info(s"creating selector $queryId")
          val queryCondition = s"query = $queryId"
          val qShardResults = shardResults.map(_.filter(queryCondition))
          val qCosts = costs match {
            case Some(cst) => Some(cst.map (_.filter (queryCondition) ))
            case None => None
          }
          val qPostingCosts = postingCosts.map(_.filter(queryCondition))
          val qImpacts = impacts.map(_.filter(queryCondition))
          val qBaseResults = baseResults.filter(queryCondition)

          val shards = for (shard <- 0 until properties.shardCount) yield {
            logger.info(s"shard $shard")
            val buckets = for (bucket <- 0 until features.properties.bucketCount) yield {
              logger.info(s"bucket $bucket")
              val bucketCondition = s"bucket = $bucket"
              val impacts = qImpacts(shard).filter(bucketCondition).select("impact")
              val impact: Float = if (impacts.count() > 0) impacts.head().getAs[Float]("impact") else 0
              val Row(cost: Float) = qCosts match {
                case Some(qc) => qc(shard).filter(bucketCondition).select("cost").head()
                case None => Row((1.0 / features.properties.bucketCount).toFloat)
              }
              val Row(postings: Long) = qPostingCosts(shard).filter(bucketCondition).select("postingcost").head()
              val bShardResults = qShardResults(shard).filter(bucketCondition)
              logger.info(s"processing results")
              val results = bShardResults
                .join(
                  qBaseResults.select($"docid-global", $"ridx" as "ridx-base"),
                  Seq("docid-global"),
                  "leftouter")
                .select("ridx", "score", "ridx-base")
                .withColumn("fixed-base", when($"ridx-base".isNotNull, $"ridx-base").otherwise(Int.MaxValue))
                .drop("ridx-base")
                .orderBy("ridx")
                .map {
                  case Row(ridx: Int, score: Float, ridxBase: Int) =>
                    Result(score, relevant = false, originalRank = ridxBase, Int.MaxValue)
                  case x => logger.error(s"couldn't match $x")
                    throw new IllegalArgumentException()
                }.collect().toSeq
              logger.info(s"results processed, creating bucket")
              Bucket(shard, results, impact, cost, postings)
            }
            new Shard(shard, buckets.toList)
          }
          new VerboseSelector(shards)
      }
  }

  def printHeader(precisions: Seq[Int], overlaps: Seq[Int], complexRecalls: Seq[Int])(writer: BufferedWriter): Unit = {
    writer.write(Seq(
      "qid",
      "step",
      "cost",
      "postings",
      "postings_relative",
      precisions.map(p => s"P@$p").mkString(","),
      overlaps.map(o => s"O@$o").mkString(","),
      complexRecalls.map(c => s"$c-CR").mkString(","),
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

  def processSelector(precisions: Seq[Int], overlaps: Seq[Int], complexRecalls: Seq[Int], maxShards: Int)
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
        complexRecalls.map(selector.complexRecall).mkString(","),
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
                      complexRecalls: Seq[Int] = Seq(10, 30),
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

      opt[Seq[Int]]('c', "complex-recalls")
        .action((x, c) => c.copy(overlaps = x))
        .text("k for which to compute k-CR")

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

        printHeader(config.precisions, config.overlaps, config.complexRecalls)(writer)

        for ((selector, idx) <- selectorsForQueries.zipWithIndex) {
          logger.info(s"processing query $idx")
          //logger.debug(s"total number of retrieved relevant documents: ${selector.shards.flatMap(_.buckets).flatMap(_.results).count(_.relevant)}")
          processSelector(config.precisions, config.overlaps, config.complexRecalls, config.maxShards)(idx, selector, writer)
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
                  originalRank: Int,
                  complexRank: Int) {
}