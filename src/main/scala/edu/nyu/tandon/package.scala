package edu.nyu

import java.io.File

import edu.nyu.tandon.search.selective._
import edu.nyu.tandon.utils.ReadLineIterator
import org.antlr.v4.runtime.atn.SemanticContext.Predicate
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.RandomStringUtils

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

  def unfolder(file: File) = {
    assert(file.exists(), "the file doesn't exist")
    val dir = file.getParentFile
    val temp = new File(file.getAbsolutePath
      .concat("-").concat(RandomStringUtils.randomAlphabetic(8)))
    FileUtils.moveDirectory(file, temp)

    val parquetFiles = temp.listFiles().filter(_.getName.endsWith("parquet"))
    assert(parquetFiles.length == 1, "detected more than one parquet file in the folder")

    val parquet = parquetFiles(0)
    FileUtils.moveFile(parquet, file)
    FileUtils.deleteDirectory(temp)
  }

}
