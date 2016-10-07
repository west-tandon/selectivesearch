package edu.nyu.tandon.search.selective.data

import edu.nyu.tandon._
import edu.nyu.tandon.search.selective._

/**
  * @author michal.siedlaczek@nyu.edu
  */
package object results {

  def resultsByShardsFromBasename(basename: String): GroupedResults = {
    val shardCount = loadProperties(basename).getProperty("shards.count").toInt
    new GroupedResults(for (s <- 0 until shardCount) yield FlatResults.fromBasename(s"$basename#$s"))
  }

  def resultsByBucketsFromBasename(basename: String): GroupedResults = {
    val shardCount = loadProperties(basename).getProperty("buckets.count").toInt
    new GroupedResults(for (s <- 0 until shardCount) yield FlatResults.fromBasename(s"$basename#$s"))
  }

  def resultsByShardsAndBucketsFromBasename(basename: String): GroupedGroupedResults = {
    val shardCount = loadProperties(basename).getProperty("shards.count").toInt
    new GroupedGroupedResults(for (s <- 0 until shardCount) yield resultsByBucketsFromBasename(s"$basename#$s"))
  }

}
