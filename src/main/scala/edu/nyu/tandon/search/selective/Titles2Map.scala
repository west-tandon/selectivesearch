package edu.nyu.tandon.search.selective

import java.io.{FileOutputStream, ObjectOutputStream}

import edu.nyu.tandon.search.selective.data.features.Features
import scopt.OptionParser

import scala.collection.immutable.HashMap

/**
  * @author michal.siedlaczek@nyu.edu
  */
object Titles2Map {

  val CommandName = "titles2map"

  def titles2map(features: Features) = {
    val map = HashMap[String, Int](features.documentTitles.toSeq.zipWithIndex:_*)
    val oos = new ObjectOutputStream(new FileOutputStream(s"${features.basename}.titlemap"))
    oos.writeObject(map)
    oos.close()
  }

  def main(args: Array[String]): Unit = {

    case class Config(basename: String = null)

    val parser = new OptionParser[Config](CommandName) {

      arg[String]("<basename>")
        .action((x, c) => c.copy(basename = x))
        .text("the prefix of the files")
        .required()

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        titles2map(Features.get(config.basename))

      case None =>
    }

  }

}
