package com.jacoffee

import com.jacoffee.util.Logging
import org.rogach.scallop.Scallop

abstract class AbstractOptions(className: String, args: Seq[String]) extends Logging {

  val options = build(className, args)
  def build(className: String, args: Seq[String]): Scallop

  def verify {
    try {
      options.verify
    } catch {
      case e: Exception =>
        logger.error(e.getMessage)
        options.printHelp
        System.exit(-1)
    }
  }

}
