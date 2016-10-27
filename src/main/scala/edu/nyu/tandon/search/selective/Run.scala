package edu.nyu.tandon.search.selective

import edu.nyu.tandon.search.selective.learn.{LearnPayoffs, PredictCosts, PredictPayoffs, TrainCosts}

/**
  * @author michal.siedlaczek@nyu.edu
  */
object Run {

  val Programs = Seq[(String, Array[String] => Unit)](
    (BucketizeResults.CommandName, BucketizeResults.main),
    (ExportSelectedToTrec.CommandName, ExportSelectedToTrec.main),
    (ResolvePayoffs.CommandName, ResolvePayoffs.main),
    (ShardSelector.CommandName, ShardSelector.main),
    (LearnPayoffs.CommandName, LearnPayoffs.main),
    (TrainCosts.CommandName, TrainCosts.main),
    (PredictPayoffs.CommandName, PredictPayoffs.main),
    (PredictCosts.CommandName, PredictCosts.main),
    (Overlap.CommandName, Overlap.main)
  )

  def printUsage(): Unit = {
    println("Usage:\n\tselsearch <program>")
    println("Available commands:\n\t" + Programs.map(_._1).sorted.mkString("\n\t"))
  }

  def main(args: Array[String]): Unit = {

    if (args.length == 0) printUsage()
    else
      Programs.find { case (x, y) => x == args.head } match {
        case None =>
          println(s"Unknown command: ${args.head}")
          printUsage()
        case Some((w, program)) => program(args.drop(1))
      }


  }

}
