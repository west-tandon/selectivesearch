package edu.nyu.tandon.search.selective.data

import edu.nyu.tandon._
import edu.nyu.tandon.utils.BulkIterator

/**
  * @author michal.siedlaczek@nyu.edu
  */
package object results {

  def resultsByShardsFromBasename(basename: String): GroupedResults = {
    val shardCount = loadProperties(basename).getProperty("shards.count").toInt
    val shards = for (s <- 0 until shardCount) yield FlatResults.fromBasename(s"$basename#$s")
    val allHaveScores = shards.map(_.hasScores).reduce(_ && _)
    val noneHasScores = shards.map(!_.hasScores).reduce(_ && _)
    require(allHaveScores || noneHasScores, "some subgroups have and some have not scores")
    new GroupedResults(new BulkIterator(shards), shardCount, allHaveScores)
  }

  def resultsByBucketsFromBasename(basename: String): GroupedResults = {
    val bucketCount = loadProperties(basename).getProperty("buckets.count").toInt
    val buckets = for (s <- 0 until bucketCount) yield FlatResults.fromBasename(s"$basename#$s")
    val allHaveScores = buckets.map(_.hasScores).reduce(_ && _)
    val noneHasScores = buckets.map(!_.hasScores).reduce(_ && _)
    require(allHaveScores || noneHasScores, "some subgroups have and some have not scores")
    new GroupedResults(new BulkIterator(buckets), bucketCount, allHaveScores)
  }

  def resultsByShardsAndBucketsFromBasename(basename: String): GroupedGroupedResults = {
    val shardCount = loadProperties(basename).getProperty("shards.count").toInt
    val shards = for (s <- 0 until shardCount) yield resultsByBucketsFromBasename(s"$basename#$s")
    val allHaveScores = shards.map(_.hasScores).reduce(_ && _)
    val noneHasScores = shards.map(!_.hasScores).reduce(_ && _)
    require(allHaveScores || noneHasScores, "some subgroups have and some have not scores")
    new GroupedGroupedResults(new BulkIterator(shards), allHaveScores)
  }

}
