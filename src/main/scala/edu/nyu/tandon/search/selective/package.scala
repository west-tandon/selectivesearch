package edu.nyu.tandon.search

import edu.nyu.tandon._
import edu.nyu.tandon.utils.ZippableSeq

import scala.language.implicitConversions
import scalax.io.Resource

/**
  * @author michal.siedlaczek@nyu.edu
  */
package object selective {

  val FieldSplitter = "\\s+"
  val FieldSeparator = " "

  val NestingIndicator = "#"

  def base(nestedBasename: String): String = nestedBasename.takeWhile(c => s"$c" != NestingIndicator)

}
