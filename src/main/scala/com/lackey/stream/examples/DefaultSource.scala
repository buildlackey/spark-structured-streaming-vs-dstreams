package com.lackey.stream.examples

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.text.{PestOptions, TextOutputWriter}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{AnalysisException, SparkSession}

/**
 * A data source for reading text files.
 */
class DefaultSource extends TextBasedFileFormat with DataSourceRegister {

  override def shortName(): String = "text2"

  override def toString: String = "Text2"

  private def verifySchema(schema: StructType): Unit = {
    if (schema.size != 1) {
      throw new RuntimeException(
        s"Text data source supports only a single column, and you have ${schema.size} columns.")
    }
    val tpe = schema(0).dataType
    if (tpe != StringType) {
      throw new RuntimeException(
        s"Text data source supports only a string column, but you have ${tpe.simpleString}.")
    }
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    val textOptions = new PestOptions(options)
    super.isSplitable(sparkSession, options, path) && !textOptions.wholeText
  }

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = Some(new StructType().add("value", StringType))

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    verifySchema(dataSchema)

    val textOptions = new PestOptions(options)
    val conf = job.getConfiguration

    textOptions.compressionCodec.foreach { codec =>
      CompressionCodecs.setCodecConfiguration(conf, codec)
    }

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new TextOutputWriter(path, dataSchema, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".txt" + CodecStreams.getCompressionExtension(context)
      }
    }
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    assert(
      requiredSchema.length <= 1,
      "Text data source only produces a single data column named \"value\".")
    val textOptions = new PestOptions(options)
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    readToUnsafeMem(broadcastedHadoopConf, requiredSchema, textOptions.wholeText)
  }

  private def readToUnsafeMem(
      conf: Broadcast[SerializableConfiguration],
      requiredSchema: StructType,
      wholeTextMode: Boolean): (PartitionedFile) => Iterator[UnsafeRow] = {

    (file: PartitionedFile) => {
      val confValue = conf.value.value
      val reader = if (!wholeTextMode) {
        new HadoopFileLinesReader(file, confValue)
      } else {
        new HadoopFileWholeTextReader(file, confValue)
      }
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => reader.close()))
      if (requiredSchema.isEmpty) {
        val emptyUnsafeRow = new UnsafeRow(0)
        reader.map(_ => emptyUnsafeRow)
      } else {
        val unsafeRow = new UnsafeRow(1)
        val bufferHolder = new BufferHolder(unsafeRow)
        val unsafeRowWriter = new UnsafeRowWriter(bufferHolder, 1)

        reader.map { line =>
          // Writes to an UnsafeRow directly
          bufferHolder.reset()
          unsafeRowWriter.write(0, line.getBytes, 0, line.getLength)
          unsafeRow.setTotalSize(bufferHolder.totalSize())
          unsafeRow
        }
      }
    }
  }
}


