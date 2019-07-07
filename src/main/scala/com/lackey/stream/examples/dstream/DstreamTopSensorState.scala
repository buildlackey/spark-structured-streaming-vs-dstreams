package com.lackey.stream.examples.dstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.immutable.TreeSet


class DstreamTopSensorState extends Serializable {

  import com.lackey.stream.examples.Constants._
  import com.lackey.stream.examples.FileHelpers._


  def processStream(stringContentStream: DStream[String],
                    outputFile: String): Unit = {
    val wordsInLine: DStream[Array[String]] =
      stringContentStream.map(_.split(","))

    val sensorStateOccurrences: DStream[(String, Int)] =
      wordsInLine.flatMap {
        words: Array[String] =>
          var retval = Array[(String, Int)]()
          if (words.length >= 4 && words(1) == "temp") {
            retval =
              words.drop(3).map((state: String) => (state, 1))
          }
          retval
      }

    val stateToCount: DStream[(String, Int)] =
      sensorStateOccurrences.
        reduceByKeyAndWindow(
          (count1: Int,
           count2: Int) => count1 + count2,
          WINDOW_DURATION, SLIDE_DURATION
        )
    val countToState: DStream[(Int, String)] =
      stateToCount.map {
        case (state, count) => (count, state)
      }

    case class TopCandidatesResult(count: Int,
                                   candidates: TreeSet[String])
    val topCandidates: DStream[TopCandidatesResult] =
      countToState.map {
        case (count, state) =>
          TopCandidatesResult(count, TreeSet(state))
      }

    val topCandidatesFinalist: DStream[TopCandidatesResult] =
      topCandidates.reduce {
        (top1: TopCandidatesResult, top2: TopCandidatesResult) =>
          if (top1.count == top2.count)
            TopCandidatesResult(
              top1.count,
              top1.candidates ++ top2.candidates)
          else if (top1.count > top2.count)
            top1
          else
            top2
      }

    topCandidatesFinalist.foreachRDD { rdd =>
      rdd.foreach {
        item: TopCandidatesResult =>
          writeStringToFile(
            outputFile,
            s"top sensor states: ${item.candidates}", true)
      }
    }
  }


  def createContext(incomingFilesDir: String,
                    checkpointDirectory: String,
                    outputFile: String): StreamingContext = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("OldSchoolStreaming")
    val ssc = new StreamingContext(sparkConf, BATCH_DURATION)
    ssc.checkpoint(checkpointDirectory)
    ssc.sparkContext.setLogLevel("ERROR")

    processStream(ssc.textFileStream(incomingFilesDir), outputFile)

    ssc
  }


  def beginProcessingInputStream(checkpointDirPath: String,
                                 incomingFilesDirPath: String,
                                 outputFile: String): StreamingContext = {
    val ssc = StreamingContext.
      getOrCreate(
        checkpointDirPath,
        () => createContext(incomingFilesDirPath, checkpointDirPath, outputFile))
    ssc.start()
    ssc
  }
}


