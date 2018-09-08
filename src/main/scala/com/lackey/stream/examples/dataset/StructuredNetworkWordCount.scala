package com.lackey.stream.examples.dataset

import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object StructuredNetworkWordCount {
  def main(args: Array[String]) {

    val host = "localhost"
    val port = 9999

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()


    // This generates error:
    // AnalysisException: Non-time-based windows are not supported on streaming DataFrames/Datasets;
    //val newLines = lines.withColumn("rank", rank().over(Window.partitionBy($"timestamp").orderBy($"timestamp".desc)))
    val newLines: Dataset[String] = lines.as[String]

    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    val grouped: KeyValueGroupedDataset[(String, String), (String, Int)] = ds.groupByKey(v => (v._1, "word"))
    val agged = grouped.mapGroups { case (g, iter) => (g._1, iter.map(_._2).sum) }


    // Start running the query that prints the running counts to the console
    val query = newLines.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
