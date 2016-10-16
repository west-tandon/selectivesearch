package edu.nyu.tandon.utils

import java.io.{BufferedReader, FileReader}

/**
  * @author michal.siedlaczek@nyu.edu
  */
class LineIterator(file: String) extends Iterator[String] {

  val reader = new BufferedReader(new FileReader(file))

  var nextLine = reader.readLine()
  var closed = false

  override def hasNext: Boolean = nextLine != null
  override def next(): String = {
    val result = nextLine
    nextLine = reader.readLine()
    if (nextLine == null && !closed) {
      reader.close()
      closed = true
    }
    result
  }
}
