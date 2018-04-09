package com.jacoffee.util

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object HDFSUtil {

  implicit def stringToPath(path: String) = new Path(path)

  def getFileSystem(uri: String): FileSystem = {
    getFileSystem(new URI(uri))
  }

  def getFileSystem(uri: URI): FileSystem = {
    val conf: Configuration = new Configuration
    FileSystem.get(uri, conf)
  }

  def getFileMetaData(path: Path) = {
    val fileSystem = getFileSystem(path.toUri)
    val contentSummary = fileSystem.getContentSummary(path)
    val fileStatus = fileSystem.getFileStatus(path)
    val fileSize = contentSummary.getLength
    val fileCount = contentSummary.getFileCount

    (fileSize, fileCount, fileStatus.getModificationTime)
  }

  def toHumanReadable(bytes: Long) = {
    // 1KB, 1MB, 1GB
    val ONE_GB = 1024 * 1024 * 1024L
    val ONE_MB = 1024 * 1024L
    val ONE_KB = 1024L

    val (result, unit) =
      if (bytes >= ONE_GB) {
        (bytes / ONE_GB, "GB")
      } else if (bytes >= ONE_MB) {
        (bytes / ONE_MB, "MB")
      } else if (bytes >= ONE_KB) {
        (bytes / ONE_KB, "KB")
      } else {
        (bytes.toString, "B")
      }

    result + unit
  }

}
