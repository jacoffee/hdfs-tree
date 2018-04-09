package com.jacoffee

import com.jacoffee.util.Logging
import org.apache.hadoop.fs.{FileSystem, PathFilter, Path}
import com.jacoffee.util.HDFSUtil._
import org.joda.time.DateTime
import org.rogach.scallop.Scallop

object HDFSTree extends Logging {

  private class Options(className: String, args: Seq[String]) extends AbstractOptions(className, args) {

    val defaultOrdering = OrderBy.values.map(_.toString)

    override def build(className: String, args: Seq[String]) = {
      val opts = Scallop(args)
          .banner(s"""Usage: hdfs-tree [OPTION]
                      |Options:
                      |""".stripMargin)
          .trailArg[String](
              name = "directory", descr = "the hdfs directory you want to traverse, eg: hdfs://localhost/",
              validate = (dir: String) => getFileSystem(dir).isDirectory(dir)
          )
          .opt[String](
            name="type", required = false,
            validate = (tpe: String) => List("d", "f").contains(tpe),
            descr = "file type to compare, d | f"
          )
          .opt[Int](
            name="depth", required = false,
            descr = "max traverse depth for directory, default 5"
          ).opt[Int](
            name="limit", required = false,
            descr = "top N to display by sort, default 5"
          ).opt[String](
            name="sort", required = false,
            validate = (sort: String) => defaultOrdering.contains(sort),
            descr = "sort options, must be size | mtime | count(file count)"
          ).opt[Boolean](
            name="reverse", descr = "reverse the sort or not"
          )

      opts
    }

    def printHelp = options.printHelp()

    lazy val depth = options.get[Int]("depth").getOrElse(5)
    lazy val limit = options.get[Int]("limit").getOrElse(5)
    lazy val sort = options.get[String]("sort").getOrElse("size")
    lazy val tpeOpt = options.get[String]("type")
    lazy val reverse = options.get[Boolean]("reverse").getOrElse(false)
    lazy val directory = options[String]("directory")

  }

  private def getFileMetaData(path: Path) = {
    val fileSystem = getFileSystem(path.toUri)
    val contentSummary = fileSystem.getContentSummary(path)
    val fileStatus = fileSystem.getFileStatus(path)
    val fileSize = contentSummary.getLength
    val fileCount = contentSummary.getFileCount

    (fileSize, fileCount, fileStatus.getModificationTime)
  }

  private def orderToOrdering(order: Order): Ordering[Path] = {
    val ordering = order.by match {
      case OrderBy.FILE_SIZE =>
        val byFileSize: Ordering[Path] = Ordering.by[Path, Long](path => getFileMetaData(path)._1)
        byFileSize
      case OrderBy.FILE_COUNT =>
        val byFileCount: Ordering[Path] = Ordering.by[Path, Long](path => getFileMetaData(path)._2)
        byFileCount
      case OrderBy.FILE_MODIFICATION =>
        val byModificationDate: Ordering[Path] = Ordering.by[Path, Long](path => getFileMetaData(path)._3)
        byModificationDate
    }

    if (order.reverse) ordering.reverse else ordering
  }

  private def orderToFormatFunc(order: Order): Path => String = {
    val YMD = "yyyy-MM-dd HH:mm:ss"
    (path: Path) => {
      val (fileSize, fileCount, fileModification) = getFileMetaData(path)

      order.by match {
        case OrderBy.FILE_SIZE => toHumanReadable(fileSize)
        case OrderBy.FILE_COUNT => fileCount.toString
        case OrderBy.FILE_MODIFICATION => new DateTime(fileModification).toString(YMD)
      }
    }
  }

  def traverse(file: Path, fileSystem: FileSystem, traverseOptions: TraverseOptions): List[String] = {
    val directoryFilter = new PathFilter {
      override def accept(pathname: Path): Boolean = fileSystem.isDirectory(pathname)
    }
    traverse(file, directoryFilter, fileSystem, traverseOptions)
  }

  def traverse(
    file: Path, fileFilter: PathFilter, fileSystem: FileSystem, traverseOptions: TraverseOptions
  ): List[String] = {
    val maxDepth = traverseOptions.maxDepth
    val order = traverseOptions.order
    val topN = traverseOptions.limit

    def createPrefixFromLevel(depth: Int, fieldValue: String, file: Path) = {
      if (depth == 0) {
        file.toString
      } else {
        val indent = List.fill(depth - 1)("\t").mkString
        "%s├── [%10s] %s".format(indent, fieldValue, file.getName)
      }
    }

    def go(file: Path, depth: Int): List[String] = {
      val fieldValue = orderToFormatFunc(order)(file)

      createPrefixFromLevel(depth, fieldValue, file) :: {
        if (fileSystem.isFile(file) || depth >= maxDepth) {
          Nil
        } else {
          val fileOrdering = orderToOrdering(order)
          val subFiles = takeOrdered(file, fileFilter, fileSystem, topN)(fileOrdering)

          subFiles.flatMap { subPath =>
            go(subPath, depth + 1)
          }
        }
      }
    }

    go(file, 0)
  }

  private def takeOrdered(
    file: Path, fileFilter: PathFilter, fileSystem: FileSystem, topN: Int
  )(ord: Ordering[Path]): List[Path] = {
    val directories = fileSystem.listStatus(file, fileFilter).map { fileStatus =>
      fileStatus.getPath
    }.toList

    directories.sorted(ord).take(topN)
  }

  private def toTraverseOptions(options: Options) = {
    val sort = options.sort
    val ordering = OrderBy.values.find(_.toString == sort).getOrElse { OrderBy.FILE_SIZE }
    TraverseOptions(Order(ordering, options.reverse), options.limit, options.depth)
  }

  private def typeOptToPathFilter(typeOpt: Option[String], fileSystem: FileSystem) = {
    new PathFilter {
      override def accept(pathname: Path): Boolean =
          typeOpt match {
            case Some(tpe) =>
              if (tpe == "d") fileSystem.isDirectory(pathname)
              else fileSystem.isFile(pathname)
            case _ => true
          }
    }
  }

  def main(args: Array[String]) = {
    val options = new Options(simpleClassName, args)

    if (args.contains("--help") || args.contains("-h")) {
      options.printHelp
    } else {
      options.verify

      var fileSystem: FileSystem = null
      try {
        fileSystem = getFileSystem(options.directory)
        val traverseOptions = toTraverseOptions(options)
        val pathFilter = typeOptToPathFilter(options.tpeOpt, fileSystem)
        traverse(options.directory, pathFilter, fileSystem, traverseOptions).foreach { println }
      } finally {
        if (fileSystem != null) {
          fileSystem.close()
        }
      }
    }
  }

}