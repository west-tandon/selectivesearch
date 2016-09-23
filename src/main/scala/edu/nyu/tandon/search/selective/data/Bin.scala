package edu.nyu.tandon.search.selective.data

/**
  * @author michal.siedlaczek@nyu.edu
  */
case class Bin(shardId: Int,
               payoff: Double,
               cost: Double) {
  def apply(shardId: Int, payoff: Double, cost: Double) = new Bin(shardId, payoff, cost)
}
