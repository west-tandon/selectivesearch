package edu.nyu.tandon.search

/**
  * @author michal.siedlaczek@nyu.edu
  */
package object selective {

  val FieldSplitter = "\\s+"
  val FieldSeparator = " "

  val NestingIndicator = "#"
  val BudgetIndicator = "$"
  val ThresholdIndicator = "%"

  def base(nestedBasename: String): String = nestedBasename
    .takeWhile(c => s"$c" != NestingIndicator)
    .takeWhile(c => s"$c" != BudgetIndicator)
    .takeWhile(c => s"$c" != ThresholdIndicator)

}
