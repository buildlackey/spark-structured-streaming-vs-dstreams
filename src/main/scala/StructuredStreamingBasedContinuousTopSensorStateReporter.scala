import java.util.concurrent.TimeUnit

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object StructuredStreamingBasedContinuousTopSensorStateReporter {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    import sparkSession.implicits._
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._


    //scala> Seq("2017-01-01 00:00:00").toDF("time").withColumn("unix_timestamp", unix_timestamp($"time")).show


    var count = 0

    def getTimestampColumn = {
      count = (count + 2) % 60
      f"2017-01-01 00:$count%02d:00"
    }


    //create stream from folder
    val fileStreamDf: Dataset[String] = sparkSession.read.text("/tmp/csv").as[String]

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

    timeStamped.cache()
    timeStamped.show()


    val counted = timeStamped.
      groupBy(
        window($"timestamp", "15 minutes").as("timewindow"), $"state").
      count().
      orderBy($"timewindow", $"count".desc)   // do we need this order by

    println("COUNTED")
    counted.show(100, false)
    println("end COUNTED")

    counted.
      withColumn("rank", rank().over(Window.partitionBy($"timewindow").orderBy($"count".desc))).
      orderBy($"timewindow", $"rank").
      show(100, false)



  }

}


