package com.lackey.stream.examples


import java.io.{FileWriter, PrintWriter}
import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}


object Constants {
  val BATCH_SECONDS: Int = 1
  val WINDOW_SECS: Int = 30 * BATCH_SECONDS
  val SLIDE_SECS: Int = 10 * BATCH_SECONDS

  val BATCH_DURATION: Duration = Seconds(BATCH_SECONDS * 1)
  val WINDOW_DURATION: Duration = Seconds(BATCH_SECONDS * WINDOW_SECS)
  val SLIDE_DURATION: Duration = Seconds(BATCH_SECONDS * SLIDE_SECS)
}
