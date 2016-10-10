package edu.nyu.tandon.search.selective

import org.apache.spark.sql.SparkSession

/**
  * @author michal.siedlaczek@nyu.edu
  */
object Spark {

  lazy val session = SparkSession.builder()
    .master("local[*]")
    .appName("selectivesearch")
    .getOrCreate()

}
