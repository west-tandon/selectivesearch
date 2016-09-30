package edu.nyu

import java.io.FileInputStream
import java.util.Properties

/**
  * @author michal.siedlaczek@nyu.edu
  */
package object tandon {

  val PropertiesSuffix = ".properties"

  def loadProperties(basename: String): Properties = {
    val properties = new Properties()
    properties.load(new FileInputStream(s"$basename$PropertiesSuffix"))
    properties
  }

}
