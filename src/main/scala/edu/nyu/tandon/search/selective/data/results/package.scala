package edu.nyu.tandon.search.selective.data

import edu.nyu.tandon._
import edu.nyu.tandon.search.selective.data.features.Features
import edu.nyu.tandon.utils.ZippedIterator

/**
  * @author michal.siedlaczek@nyu.edu
  */
package object results {

  def resultsByShardsFromBasename(basename: String): GroupedResults = {
    val features = Features.get(basename)
    val shardCount = features.shardCount
    val shards = for (s <- 0 until shardCount) yield FlatResults.fromFeatures(features, s)
    new GroupedResults(ZippedIterator(shards), shardCount)
  }

  def resultsByBucketsFromBasename(basename: String): GroupedResults = {
    val bucketCount = loadProperties(basename).getProperty("buckets.count").toInt
    val buckets = for (b <- 0 until bucketCount) yield FlatResults.fromBasename(s"$basename#$b")
    new GroupedResults(ZippedIterator(buckets).strict, bucketCount)
  }

  def resultsByShardsAndBucketsFromBasename(basename: String): GroupedGroupedResults = {
    val shardCount = Features.get(basename).shardCount
    val shards = for (s <- 0 until shardCount) yield resultsByBucketsFromBasename(s"$basename#$s")
    new GroupedGroupedResults(ZippedIterator(shards).strict)
  }

}
