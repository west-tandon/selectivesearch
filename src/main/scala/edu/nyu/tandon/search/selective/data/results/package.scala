package edu.nyu.tandon.search.selective.data

import edu.nyu.tandon._

/**
  * @author michal.siedlaczek@nyu.edu
  */
package object results {

  def resultsByShardsFromBasename(basename: String): GroupedResults = {
    val shardCount = loadProperties(basename).getProperty("shards.count").toInt
    new GroupedResults(for (s <- 0 until shardCount) yield FlatResults.fromBasename(s"$basename#$s"))
  }

  def resultsByBinsFromBasename(basename: String): GroupedResults = {
    val shardCount = loadProperties(basename).getProperty("bins.count").toInt
    new GroupedResults(for (s <- 0 until shardCount) yield FlatResults.fromBasename(s"$basename#$s"))
  }

  def resultsByShardsAndBinsFromBasename(basename: String): GroupedGroupedResults = {
    val shardCount = loadProperties(basename).getProperty("shards.count").toInt
    new GroupedGroupedResults(for (s <- 0 until shardCount) yield resultsByBinsFromBasename(s"$basename#$s"))
  }

}
