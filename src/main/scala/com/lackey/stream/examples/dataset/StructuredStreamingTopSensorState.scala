package com.lackey.stream.examples.dataset

import java.util.Date

import com.lackey.stream.examples.Constants._
import com.lackey.stream.examples.FileHelpers
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import scala.collection.immutable.SortedSet
import scala.collection.mutable
import scala.collection.mutable.WrappedArray


object TopStatesInWindowRanker {
  def rankAndFilter(batchDs: Dataset[Row]): DataFrame = {
      batchDs.
        withColumn(
          "rank",
          rank().
            over(
              Window.
                partitionBy("window_start").
                orderBy(batchDs.col("count").desc)))
        .orderBy("window_start")
        .filter(col("rank") <= 1)
        .drop("rank")
        .groupBy("window_start").agg(collect_list("state").as("states"))
  }
}

object StreamWriterStrategies {

  type DataFrameWriter = DataFrame => StreamingQuery

  val consoleWriter: DataFrameWriter = { df =>
    df.writeStream.
      outputMode("complete").
      format("console").
      trigger(Trigger.ProcessingTime(10)).
      option("truncate", value = false).
      start()
  }

  val fileWriter: DataFrameWriter = { df =>
    df
      .writeStream
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(10))
      .foreachBatch {
        (batchDs: Dataset[Row], batchId: Long) =>
          val topCountByWindowAndStateDf =
            TopStatesInWindowRanker.rankAndFilter(batchDs)
          val statesForEachWindow =
            topCountByWindowAndStateDf.
              collect().
              map {
                row: Row =>
                  val windowStart = row.getAs[Any]("window_start").toString
                  val states =
                    SortedSet[String]() ++ row.getAs[WrappedArray[String]]("states").toSet
                  s"for window $windowStart got sensor states: $states"

              }.toList

          FileHelpers.writeStringToFile(
            outputFile,
            statesForEachWindow.mkString("\n"), append = false)
          println(s"at ${new Date().toString}. Batch: $batchId / statesperWindow: $statesForEachWindow ")
      }
      .start()
  }
}

object StructuredStreamingTopSensorState {

  import StreamWriterStrategies._
  import com.lackey.stream.examples.Constants._

  def processInputStream(doWrites: DataFrameWriter = consoleWriter): StreamingQuery = {
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    import org.apache.spark.sql.functions._
    import sparkSession.implicits._

    val fileStreamDS: Dataset[String] = // create line stream from files in folder
      sparkSession.readStream.textFile(incomingFilesDirPath).as[String]

    doWrites(
      toStateCountsByWindow(
        fileStreamDS,
        sparkSession)
    )
  }


  val WINDOW: String = s"$WINDOW_SECS seconds"
  val SLIDE: String = s"$SLIDE_SECS seconds"

  def toStateCountsByWindow(linesFromFile : Dataset[String],
                            sparkSession: SparkSession):
  Dataset[Row] = {

    import sparkSession.implicits._

    val sensorTypeAndTimeDS: Dataset[(String, String)] =
      linesFromFile.flatMap {
        line: String =>
          println(s"line at ${new Date().toString}: " + line)
          val parts: Array[String] = line.split(",")
          if (parts.length >= 4 && parts(1).equals("temp")) {
            (3 until parts.length).map(colIndex => (parts(colIndex), parts(0)))
          } else {
            Nil
          }
      }

    val timeStampedDF: DataFrame =
      sensorTypeAndTimeDS.
        withColumnRenamed("_1", "state").
        withColumn(
          "timestamp",
          unix_timestamp($"_2", "yyyy-MM-dd'T'HH:mm:ss.SSS").
            cast("timestamp")).
        drop($"_2")

    System.out.println("timeStampedDF:" + timeStampedDF.printSchema());

    timeStampedDF
      .groupBy(
        window(
          $"timestamp", WINDOW, SLIDE).as("time_window"),
        $"state")
      .count()
      .withColumn("window_start", $"time_window.start")
      .orderBy($"time_window", $"count".desc)
  }
}
