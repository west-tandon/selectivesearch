package edu.nyu.tandon.search

/**
  * @author michal.siedlaczek@nyu.edu
  */
package object selective {

  val CostSuffix = ".cost"
  val DivisionSuffix = ".division"
  val PayoffSuffix = ".payoff"
  val QueriesSuffix = ".queries"
  val SelectionSuffix = ".selection"
  val SelectedSuffix = ".selected"
  val ResultsSuffix = ".results"
  val ScoresSuffix = ".scores"

  val FieldSplitter = "\\s+"
  val FieldSeparator = " "

  val NestingIndicator = "#"

  def base(nestedBasename: String): String = nestedBasename.takeWhile(c => s"$c" != NestingIndicator)

}
