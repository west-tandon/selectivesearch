package edu.nyu.tandon.search.selective

import edu.nyu.tandon.search.selective.clairvoyant.ClairvoyantSelector
import edu.nyu.tandon.search.selective.learn.{LearnPayoffs, PredictCosts, PredictPayoffs, TrainCosts}
import edu.nyu.tandon.search.selective.optimize.{BudgetOptimizer, PrecisionOptimizer}
import edu.nyu.tandon.search.selective.select.SmartSelector
import edu.nyu.tandon.search.selective.verbose.VerboseSelector
import edu.nyu.tandon.search.stat.TPaired

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
    (Overlap.CommandName, Overlap.main),
    (Time2Cost.CommandName, Time2Cost.main),
    (Selection2Time.CommandName, Selection2Time.main),
    (Penalize.CommandName, Penalize.main),
    (PrecisionOptimizer.CommandName, PrecisionOptimizer.main),
    (Titles2Map.CommandName, Titles2Map.main),
    (BudgetOptimizer.CommandName, BudgetOptimizer.main),
    (ClairvoyantSelector.CommandName, ClairvoyantSelector.main),
    (SmartSelector.CommandName, SmartSelector.main),
    (TPaired.CommandName, TPaired.main),
    (Precision.CommandName, Precision.main),
    (VerboseSelector.CommandName, VerboseSelector.main),
    (Status.CommandName, Status.main),
    (QRels2Parquet.CommandName, QRels2Parquet.main),
    (LabelResults.CommandName, LabelResults.main)
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
