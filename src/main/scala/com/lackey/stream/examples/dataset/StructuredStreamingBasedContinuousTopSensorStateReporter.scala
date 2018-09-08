package com.lackey.stream.examples.dataset

import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, Row, SparkSession}

import scala.collection.{GenTraversableOnce, immutable}
import java.sql.Timestamp


object Temp extends App {


  def mapper(timestamp: Timestamp, rows: Iterator[Row]) : immutable.Seq[String] = {
    val rowList = rows.toList
    if (rowList.isEmpty) {
      val x: immutable.Seq[String] = immutable.Seq[String]()
      x
    }
    else {
      val sortedRowList = rowList.sortBy( - _.getLong(2) /* count */)
      val maxCountForGroup = sortedRowList.head.getLong(2)


      val filtered = sortedRowList.takeWhile{
        row =>
          val i = row.getLong(2)
          val result = i == maxCountForGroup
          println(s"for maxCountForGroup = ${maxCountForGroup} and row = $row - result is $result")
          result
      }
      println(s"for timestamp $timestamp list is ${sortedRowList}")
      val result: immutable.Seq[String] = filtered.map(_.getString(1))
      result
    }
  }

  
  def mapper2(tuple: Tuple2[Timestamp, Iterator[Row]]) = {
    val (timestamp: Timestamp, rows: Iterator[Row]) = tuple
    mapper(timestamp, rows)
  }

  val row: Row = Row(1, true, "a string", null)

  private val t1 = new Timestamp(0, 0, 0, 0, 0, 0, 1)
  private val t2 = new Timestamp(0, 0, 0, 0, 0, 0, 2)
  private val t3 = new Timestamp(0, 0, 0, 0, 0, 0, 3)
  
  val data: immutable.Seq[(Timestamp, Iterator[Row])] = List(
    (t1, List[Row]().iterator),
    (t2, List(Row(0, "8", 2L), Row(0, "junk", 2L), Row(0, "fred", 1L)).iterator),
    (t3, List(Row(0, "8", 2L), Row(0, "junk", 2L), Row(0, "fred", 10L)).iterator)
  )
  
  val result: Seq[String] = data.flatMap(mapper2 _)
  
  result.foreach(println _)
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

    import org.apache.spark.sql.expressions.Window
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
      line: String =>6218
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

    //timeStamped.cache()
    //timeStamped.show()

    val timeWindow = window($"timestamp", WINDOW_DURATION, SLIDE_DURATION).as("time_window")
    val counted = timeStamped
      .groupBy(timeWindow, $"state")
      .count()
      .orderBy($"time_window", $"count".desc) // do we need this order by ?
      .withColumn("window_start", $"time_window.start")


    counted.printSchema()

    val groupedByWindowStart: KeyValueGroupedDataset[Timestamp, Row] = counted.groupByKey((row: Row) => row.getTimestamp(row.length - 1))

    val function: (Timestamp, Iterator[Row]) => String = (timestamp: Timestamp, rows: Iterator[Row]) => "foo"


    import Temp._
    val mostPopularStatesForEachTimeWindow: Dataset[Seq[String]] = groupedByWindowStart.mapGroups(mapper)

    //counted.show(100, false)
    //println("END show")

    /*

    val windowsWithRanks: Dataset[Row] = counted.
      withColumn("rank", rank().over(Window.partitionBy($"time_window").orderBy($"count".desc))).
      orderBy($"time_window", $"rank").select($"state" === "fred")
     */

    //windowsWithRanks.show(100, false)


    val query =
      mostPopularStatesForEachTimeWindow.writeStream.
        outputMode("complete").format("console").option("truncate", false).start()
    query.awaitTermination()
  }



}
