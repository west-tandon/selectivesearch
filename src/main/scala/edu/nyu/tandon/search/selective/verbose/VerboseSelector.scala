package edu.nyu.tandon.search.selective.verbose

import java.io.{BufferedWriter, File, FileWriter}

import com.typesafe.scalalogging.LazyLogging
import edu.nyu.tandon.search.selective.data.Properties
import edu.nyu.tandon.search.selective.data.features.Features
import edu.nyu.tandon.search.selective.verbose.VerboseSelector.scoreOrdering
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
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

  def selectors(basename: String, shardPenalty: Double, from: Int, to: Int): Iterator[VerboseSelector] = {
    val properties = Properties.get(basename)
    val features = Features.get(properties)
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    logger.info("loading data")

    val shardResults = for (shard <- 0 until properties.shardCount) yield {
      val parquet = spark.read.parquet(s"${features.basename}#$shard.labeledresults-${properties.bucketCount}")
      val columns = Seq("query", "bucket", "score", "relevant", "baseorder", "complexorder")
        .intersect(parquet.schema.fieldNames)
      val relevantExists = columns.contains("relevant")
      val baseOrderExists = columns.contains("baseorder")
      val complexOrderExists = columns.contains("complexorder")
      parquet
        .select(columns.head, columns.drop(1):_*)
        .filter($"query" >= from and $"query" < to)
        .map (row => {
          val query = row.getInt(0)
          val bucket = row.getInt(1)
          val score = row.getDouble(2)
          val relevant = if (relevantExists) row.getAs[Boolean]("relevant") else false
          val baseOrder = if (baseOrderExists) row.getAs[Int]("baseorder") else Int.MaxValue
          val complexOrder = if (complexOrderExists) row.getAs[Int]("complexorder") else Int.MaxValue
          (query, bucket, score, relevant, baseOrder, complexOrder)
        }).collect()
        .groupBy(_._1)
        .mapValues(_.groupBy(_._2).mapValues(_.map {
          case (_, _, score, relevant, baseOrder, complexOrder) =>
            Result(score, relevant, baseOrder, complexOrder)
        }))
    }

    //val costs =
    //  if (new File(s"basename#0.cost").exists())
    //    Some(for (shard <- 0 until properties.shardCount) yield
    //      spark.read.parquet(s"$basename#$shard.cost"))
    //  else None

    val postingCosts = for (shard <- 0 until properties.shardCount) yield
      spark.read.parquet(s"${features.basename}#$shard.postingcost-${properties.bucketCount}")
        .select($"query", $"bucket", $"postingcost")
        .filter($"query" >= from and $"query" < to)
        .map {
          case Row(query: Int, bucket: Int, postingCost: Long) => (query, bucket, postingCost)
        }.collect()
        .groupBy(_._1)
        .mapValues(_.groupBy(_._2).mapValues(postingCostList => {
          assert(postingCostList.length == 1,
            s"there must be exactly 1 postingCost value for (query, shard, bucket) but ${postingCostList.length} found")
          postingCostList.head._3
        }))

    val impacts = for (shard <- 0 until properties.shardCount) yield
      spark.read.parquet(s"$basename#$shard.impacts")
        .select($"query", $"bucket", $"impact")
        .filter($"query" >= from and $"query" < to)
        .map (row => (row.getAs[Int]("query"), row.getAs[Int]("bucket"), row.getAs[Double]("impact"))).collect()
        .groupBy(_._1)
        .mapValues(_.groupBy(_._2).mapValues(impactList => {
          assert(impactList.length == 1,
            s"there must be exactly 1 impact value for (query, shard, bucket) but ${impactList.length} found")
          impactList.head._3
        }))

    val queryFeatures = spark.read.parquet(s"${features.basename}.queryfeatures")
      .filter($"query" >= from and $"query" < to)

    logger.info("data loaded")

    queryFeatures.select("query")
      .orderBy("query")
      .collect()
      .toIterator
      .map {
        case Row(queryId: Int) =>

          logger.info(s"creating selector $queryId")

          val queryCondition = s"query = $queryId"

          val shards = for (shard <- 0 until properties.shardCount) yield {
            val qResults = shardResults(shard).get(queryId)
            val qImpacts = impacts(shard).get(queryId)
            val qPostingCosts = postingCosts(shard)(queryId)
            val buckets = for (bucket <- 0 until properties.bucketCount) yield {
              Bucket(shard,
                qResults match {
                  case Some(someResults) => someResults.get(bucket) match {
                    case Some(results) => results
                    case None => Seq()
                  }
                  case None => Seq()
                },
                qImpacts match {
                  case Some(someImpacts) => someImpacts.get(bucket) match {
                    case Some(impact) => impact
                    case None => 0.0
                  }
                  case None => 0.0
                },
                cost = 1.0 / properties.bucketCount,
                qPostingCosts(bucket))
            }
            Shard(shard, buckets.toList)
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

      //logger.info(s"Selected [shard=${selector.lastSelectedShard}, bucket=${selector.lastSelectedBucket}, cost=${selector.lastSelectedCost}]")

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
                      shardPenalty: Double = 0.0,
                      batchSize: Int = 200)

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

      opt[Int]('b', "batch-size")
        .action((x, c) => c.copy(batchSize = x))
        .text("how many queries to run at once in memory")

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        assert(config.batchSize > 0)

        val features = Features.get(Properties.get(config.basename))
        val queries = SparkSession.builder().master("local").getOrCreate()
          .read.parquet(s"${features.basename}.queryfeatures")
          .select("query")
          .orderBy("query")
          .collect()
          .map(_.getInt(0))
          .grouped(config.batchSize)
          .map(a => (a.head, a.last + 1))

        val writer = new BufferedWriter(new FileWriter(s"${config.basename}.verbose"))
        printHeader(config.precisions, config.overlaps, config.complexRecalls)(writer)

        for ((from, to) <- queries) {

          logger.info(s"processing batch [$from, $to]")
          val selectorsForQueries = selectors(config.basename, config.shardPenalty, from, to)

          for ((selector, idx) <- selectorsForQueries.zipWithIndex) {
            logger.info(s"processing query ${idx + from}")
            processSelector(config.precisions, config.overlaps, config.complexRecalls, config.maxShards)(idx, selector, writer)
          }
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