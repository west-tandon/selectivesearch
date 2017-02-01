package edu.nyu.tandon.search.selective.data

import java.io.FileInputStream

import edu.nyu.tandon.search.selective._
import edu.nyu.tandon.search.selective.data.features.Features

/**
  * @author michal.siedlaczek@nyu.edu
  */
class Properties(val file: String) {

  lazy val props: java.util.Properties = {
    val properties = new java.util.Properties()
    properties.load(new FileInputStream(file))
    properties
  }

  def getProperty(property: String): String = {
    val value = props.getProperty(property)
    require(value != null, s"property $property is not defined")
    value
  }

  lazy val featuresPath: String = getProperty("features")
  lazy val bucketCount: Int = getProperty("buckets.count").toInt
  lazy val k: Int = getProperty("k").toInt
  lazy val queryPayoffFeaturesNames: List[String] = getProperty("features.payoff.query").split(",").map(_.trim).toList
  lazy val shardPayoffFeaturesNames: List[String] = getProperty("features.payoff.shard").split(",").map(_.trim).toList
  lazy val bucketPayoffFeaturesNames: List[String] = getProperty("features.payoff.bucket").split(",").map(_.trim).toList
  lazy val payoffLabel: String = getProperty("features.payoff.label")
  lazy val features: Features = Features.get(this)

}

object Properties {
  def get(basename: String): Properties = {
    new Properties(s"${base(basename)}.properties")
  }
}