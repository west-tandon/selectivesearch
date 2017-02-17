package edu.nyu

import edu.nyu.tandon.search.selective._
import edu.nyu.tandon.utils.ReadLineIterator
import org.antlr.v4.runtime.atn.SemanticContext.Predicate

import scala.collection.mutable.ListBuffer

/**
  * @author michal.siedlaczek@nyu.edu
  */
package object tandon {

  def takeUntilOrNil[A](list: List[A])(p: A => Boolean): List[A] = {
    val b = new ListBuffer[A]
    var these = list
    while (these.nonEmpty && !p(these.head)) {
      b += these.head
      these = these.tail
    }
    if (these.nonEmpty) {
      b += these.head
      b.toList
    }
    else Nil
  }

}
