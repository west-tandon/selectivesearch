package edu.nyu.tandon.test

import java.nio.file.{Files, Path, Paths}

import edu.nyu.tandon.utils.Lines
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{DirectoryFileFilter, OrFileFilter, RegexFileFilter}
import org.scalatest.FunSuite
import org.scalatest.Matchers._

/**
  * @author michal.siedlaczek@nyu.edu
  */
class BaseFunSuite extends FunSuite {

  def compareFilesBetweenDirectories(fileNames: Seq[String], dir1: String, dir2: String): Unit =
    for (file <- fileNames) {
      val f1 = s"$dir1/$file"
      val f2 = s"$dir2/$file"
      Lines.fromFile(f1).toSeq should contain theSameElementsInOrderAs Lines.fromFile(f2).toSeq
    }

  def createTemporaryDir(): Path = Files.createTempDirectory(null)

  def createTemporaryCopyOfResources(regex: String): Path = {
    val tmpDir = Files.createTempDirectory(null)
    FileUtils.copyDirectory(Paths.get(getClass.getResource("/").getPath).toFile, tmpDir.toFile,
      new OrFileFilter(new RegexFileFilter(regex), DirectoryFileFilter.INSTANCE))
    tmpDir
  }

  def resourcesPath: String = getClass.getResource("/").getPath

}
