package com.lackey.stream.examples.dataset

import java.io.{File, FileWriter}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{ForeachWriter, Row}


class RowWriter extends ForeachWriter[Row] { // mine

  var fileWriter: FileWriter = _

  override def process(value: Row): Unit = {
    fileWriter.append(value.toSeq.mkString(","))
  }

  override def close(errorOrNull: Throwable): Unit = {
    fileWriter.close()
  }

  override def open(partitionId: Long, version: Long): Boolean = {
    FileUtils.forceMkdir(new File(s"src/test/resources/${partitionId}"))
    fileWriter = new FileWriter(new File(s"src/test/resources/${partitionId}/temp"))
    true
  }
}

