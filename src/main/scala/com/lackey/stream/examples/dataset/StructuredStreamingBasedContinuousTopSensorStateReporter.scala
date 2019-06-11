package com.lackey.stream.examples.dataset

import java.sql.Timestamp

import org.apache.spark.sql._

import scala.collection.{GenTraversableOnce, immutable}
import java.sql.Timestamp


object MostPopularStateForGroupFinder extends App {

  /**
    * Given a sequence of rows associated with the start time of a time window, this method
    * finds the most popular states reported for probe sensors in that time window.
    *
    * Each Row is interpreted as Row( timestamp, stateName, countForStateInThisWindow.)
    */
  def findMostPopularState(timestamp: Timestamp, rows: Iterator[Row]) : immutable.Seq[String] = {
    val rowList = rows.toList
    if (rowList.isEmpty) {
      immutable.Seq[String]()
    }
    else {
      val sortedRowList = rowList.sortBy( - _.getLong(2) /* order by countForStateInThisWindow */)
      val maxCountForGroup = sortedRowList.head.getLong(2)
      val filtered = sortedRowList.takeWhile{ row => row.getLong(2) == maxCountForGroup }
      filtered.map(_.getString(1))
    }
  }


  /**
    * This method exists to support unit testing
    */
  def findMostPopularState(tuple: Tuple2[Timestamp, Iterator[Row]]): immutable.Seq[String] = {
    val (timestamp: Timestamp, rows: Iterator[Row]) = tuple
    findMostPopularState(timestamp, rows)
  }
}


object StructuredStreamingBasedContinuousTopSensorStateReporter {


  val WINDOW_DURATION = "15 minutes"
  val SLIDE_DURATION = "15 minutes"

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    import org.apache.spark.sql.functions._
    import sparkSession.implicits._


    var count = 0

    def getTimestampColumn = {
      count = (count + 2) % 60
      f"2017-01-01 00:$count%02d:00"
    }

    //create stream from folder
    val fileStreamDf: Dataset[String] = sparkSession.readStream.textFile("/tmp/input.dir").as[String]

    val result: Dataset[(String, String)] = fileStreamDf.flatMap {
      line: String =>
        val parts: Array[String] = line.split(",")
        if (parts.length >= 3 && parts(0).equals("probe")) {
          (2 until parts.length).map(colIndex => (parts(colIndex), getTimestampColumn))
        } else {
          Nil
        }
    }

    val timeStamped =
      result.
        withColumnRenamed("_1", "state").
        withColumn("timestamp", unix_timestamp($"_2").cast("timestamp")).
        drop($"_2")

    val timeWindow = window($"timestamp", WINDOW_DURATION, SLIDE_DURATION).as("time_window")
    val counted: DataFrame = timeStamped
      .groupBy(timeWindow, $"state")
      .count()
      .orderBy($"time_window", $"count".desc) // do we need this order by ?
      .withColumn("window_start", $"time_window.start")

    counted.printSchema()

    val groupedByWindowStart: KeyValueGroupedDataset[Timestamp, Row] =
      counted.groupByKey((row: Row) => row.getTimestamp(row.length - 1))

    import MostPopularStateForGroupFinder._
    val mostPopularStatesForEachTimeWindow: Dataset[Seq[String]] =
      groupedByWindowStart.mapGroups(findMostPopularState)


    val query =
      mostPopularStatesForEachTimeWindow.writeStream.
        outputMode("complete").format("console").option("truncate", false).start()
    query.awaitTermination()
  }



}
