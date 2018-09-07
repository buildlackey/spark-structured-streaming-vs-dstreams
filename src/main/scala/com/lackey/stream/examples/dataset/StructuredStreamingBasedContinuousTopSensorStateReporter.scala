package com.lackey.stream.examples.dataset

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}

import scala.tools.nsc.interpreter.Completion.Candidates


case class Candidate(timeStamp: String, state: String, count: Long)

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



    var counter = 0

    def getTimestampColumn = {
      counter = (counter + 2) % 60
      f"2017-01-01 00:$counter%02d:00"
    }

    //create stream from folder
    val fileStreamDf: Dataset[String] = sparkSession.readStream.textFile("/tmp/input.dir").as[String]

    val result: Dataset[(String, String)] = fileStreamDf.flatMap {
      line: String =>
        val parts: Array[String] = line.split(",")
        if (parts.length >= 3 && parts(0).equals("probe")) {
          (2 until parts.length).map ( colIndex => (parts(colIndex), getTimestampColumn) )
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

    val windowsWithRanks: Dataset[Row] =
      timeStamped.
        groupBy(window($"timestamp", WINDOW_DURATION) as "group", $"state").
        agg(count("state") as "count").
        orderBy($"group", $"count".desc)

        //withColumn("rank", rank().over(Window.partitionBy($"start").orderBy($"count".desc)))



    val windowsWithRanks: Dataset[Row] =
      timeStamped.
        groupBy(window($"timestamp", WINDOW_DURATION) as "group", $"state").
        agg(count("state") as "count").
        orderBy($"group", $"count".desc)



    windowsWithRanks.printSchema()


    val mapped: Dataset[Candidate] = windowsWithRanks.map(row => Candidate(row.get(0).toString, row.getString(1), row.getLong(2)))

    val mapped2: RDD[Candidate] = mapped.rdd.map(c => Candidate(c.timeStamp, c.state, c.count +100))

    val mapped3 = sparkSession.createDataFrame(mapped2)


    val writer = new RowWriter()
    val query =
      mapped3.writeStream. outputMode("complete") .
        format("console").option("truncate", false).start()

    query.awaitTermination()


      //foreach(writer).start()

  }

}
/*


  count(). orderBy($"timewindow", $"count".desc)   // do we need this order by


//counted.show(100, false)
//println("END show")

val windowsWithRanks: Dataset[Row] = counted.
withColumn("rank", rank().over(Window.partitionBy($"timewindow").orderBy($"count".desc))).
orderBy($"timewindow", $"rank")

//windowsWithRanks.show(100, false)

 */
