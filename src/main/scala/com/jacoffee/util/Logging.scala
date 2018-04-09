package com.jacoffee.util

import org.slf4j.LoggerFactory

trait Logging {

  // Method to get the logger name for this object
  protected def simpleClassName = {
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")
  }

  lazy val logger = LoggerFactory.getLogger(simpleClassName)

}

