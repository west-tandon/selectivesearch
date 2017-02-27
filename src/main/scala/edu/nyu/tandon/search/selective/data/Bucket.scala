package edu.nyu.tandon.search.selective.data

/**
  * @author michal.siedlaczek@nyu.edu
  */
case class Bucket(shardId: Int,
                  payoff: Double,
                  cost: Double,
                  penalty: Double = 0.0) {
}
