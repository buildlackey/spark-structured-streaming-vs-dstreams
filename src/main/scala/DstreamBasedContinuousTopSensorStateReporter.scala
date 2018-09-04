/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
import java.io.{File, FileOutputStream, PrintWriter}
import java.nio.charset.Charset
import java.util.Calendar

import com.google.common.io.Files
import java.io.FileWriter
import java.io.PrintWriter
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext, Time}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


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

    // Find all of probe's states (order by most frequently occuring to least)
    val probeHeroOccurrences: DStream[(String, Int)] =
      lines.flatMap {
        wordsInLine: Array[String] =>
          val fan = wordsInLine(0)
          var retval = Array[(String,Int)]()
          if (fan == "probe") {
            retval = wordsInLine.drop(2).map((hero: String) => (hero,1))
          } else {
            retval = Array[(String,Int)]()     // skip non-probe records
          }
          retval
      }

    val heroToCount: DStream[(String, Int)] =
      probeHeroOccurrences.
        reduceByKeyAndWindow((count1:Int, count2:Int)=> count1 + count2, WINDOW_DURATION, SLIDE_DURATION)
    val countToHero: DStream[(Int, String)] = heroToCount.map{ case(hero,count) => (count,hero) }

    case class TopCandidatesResult(hero: String,
                                   count: Int,
                                   candidates: Set[String] /* all candidates seen 'count' times*/)
    val topCandidates: DStream[TopCandidatesResult] =
      countToHero.map{ case (count, hero) => TopCandidatesResult(hero, count, Set(hero)) }

    val topCandidatesFinalist: DStream[TopCandidatesResult] =  topCandidates.reduce {
      (top1:TopCandidatesResult, top2:TopCandidatesResult) =>
        if (top1.count == top2.count)
          TopCandidatesResult(top1.hero, top1.count, top1.candidates ++ top2.candidates)
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


