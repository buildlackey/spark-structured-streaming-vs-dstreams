package com.lackey.stream.examples.dataset

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._


import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.MicroBatchExecution
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery, Trigger}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.types.{StringType, StructType}

import scala.collection.immutable


object WriterStrategies {
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
    df.writeStream.
      outputMode("complete").
      trigger(Trigger.ProcessingTime(10)).

      foreachBatch {
        (batchDs: Dataset[Row], batchId: Long) =>
          val topCountByWindowAndStateDf =
            batchDs.
              withColumn(
                "rank",
                rank().over(Window.partitionBy("window_start").orderBy(batchDs.col("count").desc)))
              .orderBy("window_start")
              .filter(col("rank") <= 1)
              .drop("rank")
              .groupBy("window_start").agg(collect_list("state").as("states"))

          topCountByWindowAndStateDf.printSchema()
          topCountByWindowAndStateDf.show(truncate = false)


          val states: Array[(String, String)] =
            topCountByWindowAndStateDf.
              collect().
              map {
                row: Row =>
                  println("row:" + row);
                  println("row.getAs[String](\"window_start\"):" + row.getAs[String]("window_start"));

                  val x= (row.getAs[String]("window_start"), row.getAs[String]("states"))
                  System.out.println("x:" + x);
                  x
              }
          System.out.println("states:" + states.toList);
        //val topCountByWindowAndState = topCountByWindowAndStateDf.collect().map(_.getString(0)).toSet
        //System.out.println("topCountByWindowAndState:" + topCountByWindowAndState);
      }
      .start()
  }

  class WindowWriter extends ForeachWriter[Row] {
    override def open(partitionId: Long, version: Long): Boolean = true

    override def process(value: Row): Unit = {
      value.schema.printTreeString()

    }

    override def close(errorOrNull: Throwable): Unit = {}
  }

}

object StructuredStreamingTopSensorStateReporter {

  import com.lackey.stream.examples.Constants._
  import WriterStrategies._

  val WINDOW: String = s"$WINDOW_SECS seconds"
  val SLIDE: String = s"$SLIDE_SECS seconds"

  def processInputStream(doWrites: DataFrameWriter = consoleWriter): StreamingQuery = {
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    //Logger.getLogger(classOf[MicroBatchExecution]).setLevel(Level.DEBUG)

    import org.apache.spark.sql.functions._
    import sparkSession.implicits._

    val fileStreamDS: Dataset[String] = // create line stream from files in folder
      sparkSession.readStream.textFile(incomingFilesDirPath).as[String]

    val sensorTypeAndTimeDS: Dataset[(String, String)] =
      fileStreamDS.flatMap {
        line: String =>
          println(s"line at ${new Date().toString}: " + line)
          val parts: Array[String] = line.split(",")
          if (parts.length >= 4 && parts(1).equals("probe")) {
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


    val timeWindow = window($"timestamp", WINDOW, SLIDE).as("time_window")
    val counted: DataFrame = timeStampedDF
      .groupBy(timeWindow, $"state")
      .count()
      .withColumn("window_start", $"time_window.start")
      .orderBy($"time_window", $"count".desc)

    /*
    val groupedByWindowStart: KeyValueGroupedDataset[Timestamp, Row] =
      counted.groupByKey((row: Row) => row.getTimestamp(row.length - 1))

    import MostPopularStateForGroupFinder._
    val mostPopularStatesForEachTimeWindow: Dataset[Seq[String]] =
      groupedByWindowStart.mapGroups(findMostPopularState)

     */


    //mostPopularStatesForEachTimeWindow.writeStream.
    //outputMode("complete").format("console").option("truncate", false).start()

    doWrites(counted)
  }
}
