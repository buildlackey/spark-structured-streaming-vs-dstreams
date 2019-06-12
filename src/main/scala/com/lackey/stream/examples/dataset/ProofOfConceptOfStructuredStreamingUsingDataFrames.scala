package com.lackey.stream.examples.dataset

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

object ProofOfConceptOfStructuredStreamingUsingDataFrames {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local").appName("example").getOrCreate()
    import sparkSession.implicits._

    var count = 0

    def getTimestampColumn = {
      count = (count + 2) % 60
      f"2017-01-01 00:$count%02d:00"
    }

    val fileStreamDf: Dataset[String] = sparkSession.read.text("/tmp/csv").as[String]

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

    val counted = timeStamped.
      groupBy(
        window($"timestamp", "15 minutes").as("timewindow"), $"state").
      count().
      orderBy($"timewindow", $"count".desc) // do we need this order by

    counted.
      withColumn("rank", rank().
        over(Window.partitionBy($"timewindow").orderBy($"count".desc))).
      orderBy($"timewindow", $"rank").
      show(100, false)
  }
}



