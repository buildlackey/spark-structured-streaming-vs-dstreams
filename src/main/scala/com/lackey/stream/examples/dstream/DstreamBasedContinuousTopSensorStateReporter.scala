package com.lackey.stream.examples.dstream

import java.io.{FileWriter, PrintWriter}
import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}


class DstreamBasedContinuousTopSensorStateReporter extends Serializable  {
  val BATCH_SECONDS = 1
  val BATCH_DURATION: Duration = Seconds(BATCH_SECONDS * 1)
  val WINDOW_DURATION: Duration = Seconds(BATCH_SECONDS * 30)
  val SLIDE_DURATION: Duration = Seconds(BATCH_SECONDS * 10)


  def writeStringToFile(outputPath: String,
                        content: String): Unit = {
    val fileWriter = new FileWriter(outputPath, true)
    val printWriter = new PrintWriter(fileWriter)
    val timestampedOutput = s"${Calendar.getInstance.getTime}: $content"
    printWriter.println(timestampedOutput);
    fileWriter.close()
  }


  def processStream(stringContentStream: DStream[String], outputFile: String): Unit = {
    val lines: DStream[Array[String]] = stringContentStream.map(_.split(","))

    // Filter for 'probe' type of sensor, then find all of probe's states (order by most frequently occurring to least)
    val sensorStateOccurrences: DStream[(String, Int)] =
      lines.flatMap {
        wordsInLine: Array[String] =>
          val sensorType = wordsInLine(0)
          var retval = Array[(String,Int)]()
          if (sensorType == "probe") {
            retval = wordsInLine.drop(2).map((state: String) => (state,1))
          } else {
            retval = Array[(String,Int)]()     // skip non-probe records
          }
          retval
      }

    val stateToCount: DStream[(String, Int)] =
      sensorStateOccurrences.
        reduceByKeyAndWindow((count1:Int, count2:Int)=> count1 + count2, WINDOW_DURATION, SLIDE_DURATION)
    val countToState: DStream[(Int, String)] = stateToCount.map{ case(state,count) => (count,state) }

    case class TopCandidatesResult(state: String,
                                   count: Int,
                                   candidates: Set[String] /* all candidates seen 'count' times*/)
    val topCandidates: DStream[TopCandidatesResult] =
      countToState.map{ case (count, state) => TopCandidatesResult(state, count, Set(state)) }

    val topCandidatesFinalist: DStream[TopCandidatesResult] =  topCandidates.reduce {
      (top1:TopCandidatesResult, top2:TopCandidatesResult) =>
        if (top1.count == top2.count)
          TopCandidatesResult(top1.state, top1.count, top1.candidates ++ top2.candidates)
        else if (top1.count > top2.count)
          top1
        else
          top2
    }

    topCandidatesFinalist.foreachRDD{ rdd =>
      rdd.foreach{
        item: TopCandidatesResult =>
          writeStringToFile(
            outputFile,
            s"top sensor states: ${item.candidates}")
      }
    }
  }


  def createContext(incomingFilesDir: String,
                    checkpointDirectory: String,
                    outputFile: String) : StreamingContext = {
    // If you do not see this printed, that means the StreamingContext has been loaded from new checkpoint
    println("Creating new context")

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


