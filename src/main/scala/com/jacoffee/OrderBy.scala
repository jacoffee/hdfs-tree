package com.jacoffee

object OrderBy extends Enumeration {
  val FILE_SIZE = Value(0, "size")
  val FILE_COUNT = Value(1, "count")
  val FILE_MODIFICATION = Value(2, "mtime")
}

case class Order(by: OrderBy.Value, reverse: Boolean = true)
case class TraverseOptions(order: Order, limit: Int, maxDepth: Int)
