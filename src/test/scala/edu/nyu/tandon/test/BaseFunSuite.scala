package edu.nyu.tandon.test

import java.nio.file.{Files, Path, Paths}

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.RegexFileFilter
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.io.Source

/**
  * @author michal.siedlaczek@nyu.edu
  */
class BaseFunSuite extends FunSuite {

  def compareFilesBetweenDirectories(fileNames: Seq[String], dir1: String, dir2: String): Unit =
    for (file <- fileNames) {
      val f1 = s"$dir1/$file"
      val f2 = s"$dir2/$file"
      Source.fromFile(f1).getLines().toSeq should contain theSameElementsInOrderAs Source.fromFile(f2).getLines().toSeq
    }

  def createTemporaryCopyOfResources(regex: String): Path = {
    val tmpDir = Files.createTempDirectory(null)
    FileUtils.copyDirectory(Paths.get(getClass.getResource("/").getPath).toFile, tmpDir.toFile, new RegexFileFilter(regex))
    tmpDir
  }

}
