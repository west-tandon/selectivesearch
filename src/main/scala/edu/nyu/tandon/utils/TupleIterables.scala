package edu.nyu.tandon.utils

import scala.language.implicitConversions

/**
  * @author michal.siedlaczek@nyu.edu
  */
object TupleIterables {
  implicit def seq2iterator[A1](seq: Seq[A1]): Iterator[A1] = seq.iterator
  implicit def seq2iterator[A1, A2](seq: Seq[(A1, A2)]): Tuple2Iterator[A1, A2] = new Tuple2Iterator(seq.iterator)
  implicit def seq2iterator[A1, A2, A3](seq: Seq[(A1, A2, A3)]): Tuple3Iterator[A1, A2, A3] = new Tuple3Iterator(seq.iterator)
  implicit def seq2iterator[A1, A2, A3, A4](seq: Seq[(A1, A2, A3, A4)]): Tuple4Iterator[A1, A2, A3, A4] = new Tuple4Iterator(seq.iterator)
  implicit def seq2iterator[A1, A2, A3, A4, A5](seq: Seq[(A1, A2, A3, A4, A5)]): Tuple5Iterator[A1, A2, A3, A4, A5] = new Tuple5Iterator(seq.iterator)
  implicit def seq2iterator[A1, A2, A3, A4, A5, A6](seq: Seq[(A1, A2, A3, A4, A5, A6)]): Tuple6Iterator[A1, A2, A3, A4, A5, A6] = new Tuple6Iterator(seq.iterator)
}
