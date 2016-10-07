package edu.nyu.tandon.search.selective.data

/**
  * @author michal.siedlaczek@nyu.edu
  */
case class Bucket(shardId: Int,
                  payoff: Double,
                  cost: Double) {
  def apply(shardId: Int, payoff: Double, cost: Double) = new Bucket(shardId, payoff, cost)
}
