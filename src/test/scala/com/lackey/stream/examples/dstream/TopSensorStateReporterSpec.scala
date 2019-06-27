package com.lackey.stream.examples.dstream

import java.io.{File, PrintWriter}
import java.time.Instant
import java.util.Date

import com.lackey.stream.examples.dataset.StructuredStreamingTopSensorState
import org.apache.commons.io.FileUtils
import org.apache.spark.streaming.StreamingContext
import org.scalatest.{Assertion, Matchers, WordSpec}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class TopSensorStateReporterSpec extends WordSpec with Matchers {
  import com.lackey.stream.examples.Constants._

  val t2_input_path = s"$incomingFilesDirPath/t2_probe_x2_2"
  val t7_input_path = s"$incomingFilesDirPath/t7_probe_x2_1"
  val t12_input_path = s"$incomingFilesDirPath/t12_probe_x1_2"

  def now = java.time.LocalDateTime.now().toString()

  def t2_probe_x2_2 =
    s"""$now,probe,oakland,x1,x2
       |$now,f,freemont,x3,x4
       |$now,probe,cupertino,x2,x4
    """.stripMargin

  def t7_probe_x2_1 =
    s"""$now,probe,hayward,x2
       |$now,m,vallejo,x2,x3
    """.stripMargin


  def t12_probe_x1_2 =
    s"""$now,probe,milpitas,x1
       |$now,m,berkeley,x9
       |$now,probe,burlingame,x1
    """.stripMargin


  def dummy =
    s"""$now,zoop,oakland,x1
       |$now,rockbox,oakland,x1
    """.stripMargin

  "Top Sensor State Reporter" should {
    "correctly output top states for target sensor using DStreams" in {
      setup()

      val ctx: StreamingContext =
        new DstreamBasedContinuousTopSensorStateReporter().
          beginProcessingInputStream(checkpointDirPath, incomingFilesDirPath, outputFile)

      writeRecords()
      verifyResult
      ctx.stop()
    }

    "correctly output top states for target sensor using structured streaming" in {
      import com.lackey.stream.examples.dataset.WriterStrategies._

      setup()

      var timeStampSeconds = Instant.now.getEpochSecond
      while (timeStampSeconds % 30 != 0) {  timeStampSeconds = Instant.now.getEpochSecond }
      println(s"start processing on 0 or 30 second boundary  ${new Date().toString}")

      val queryFuture =
        Future {
          Thread.sleep(10 * 1000)
          StructuredStreamingTopSensorState.processInputStream( doWrites = fileWriter)
        }

      writeRecords()
      val query = Await.result(queryFuture , 6 seconds)
      verifyResult
      query.stop()
    }
  }


  def writeStringToFile(filePath: String, content: String): Unit = {
    new PrintWriter(filePath) {
      write(content); close()
    }
  }

  def verifyResult: Assertion = {
    val output: mutable.Seq[String] = FileUtils.readLines(new File(outputFile)).asScala
    val strings: mutable.Seq[String] = output.map(_.replaceAll(".*sensor states: ", ""))
    val expected = "Set(x2)|Set(x1, x2)|Set(x1, x2)|Set(x1)"
    strings.mkString("|") shouldBe expected
  }

  def writeRecords(): Unit = {
    Thread.sleep(2 * 1000) //
    println(s"wrote to file at ${new Date().toString}")
    writeStringToFile(t2_input_path, t2_probe_x2_2)

    Thread.sleep(5 * 1000) //
    println(s"wrote to file2 at ${new Date().toString}")
    writeStringToFile(t7_input_path, t7_probe_x2_1)

    Thread.sleep(5 * 1000) //
    println(s"wrote to file3 at ${new Date().toString}")
    writeStringToFile(t12_input_path, t12_probe_x1_2)


    Thread.sleep(30 * 1000) //
  }

  // Delete and recreate checkpoint and input directories and any old version of output file
  def setup(): Boolean = {
    new File(outputFile).delete()
    FileUtils.deleteDirectory(new File(checkpointDirPath))
    FileUtils.deleteDirectory(new File(incomingFilesDirPath))
    new File(checkpointDirPath).mkdirs()
    new File(incomingFilesDirPath).mkdirs()
  }
}