package com.lackey.stream.examples.dstream

import java.io.{File, PrintWriter}
import java.util.Date

import com.lackey.stream.examples.dataset.StructuredStreamingTopSensorState
import org.apache.commons.io.FileUtils
import org.apache.spark.streaming.StreamingContext
import org.scalatest.{Assertion, Matchers, WordSpec}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

class TopSensorStateReporterSpec extends WordSpec with Matchers {

  import com.lackey.stream.examples.Constants._

  val t2_input_path = s"$incomingFilesDirPath/t2_temp_x2_2"
  val t7_input_path = s"$incomingFilesDirPath/t7_temp_x2_1"
  val t12_input_path = s"$incomingFilesDirPath/t12_temp_x1_2"

  val StructuredStreamStartupWaitSeconds = 0

  def now = java.time.LocalDateTime.now().toString()

  def t2_temp_x2_2 =
    s"""$now,temp,oakland,x1,x2
       |$now,f,freemont,x3,x4
       |$now,temp,cupertino,x2,x4
    """.stripMargin

  def t7_temp_x2_1 =
    s"""$now,temp,hayward,x2
       |$now,m,vallejo,x2,x3
    """.stripMargin


  def t12_temp_x1_2 =
    s"""$now,temp,milpitas,x1
       |$now,m,berkeley,x9
       |$now,temp,burlingame,x1
    """.stripMargin


  def writeRecords(): Unit = {
    Thread.sleep(2 * 1000) //
    println(s"wrote to file at ${new Date().toString}")
    writeStringToFile(t2_input_path, t2_temp_x2_2)

    Thread.sleep(5 * 1000) //
    println(s"wrote to file2 at ${new Date().toString}")
    writeStringToFile(t7_input_path, t7_temp_x2_1)

    Thread.sleep(5 * 1000) //
    println(s"wrote to file3 at ${new Date().toString}")
    writeStringToFile(t12_input_path, t12_temp_x1_2)

    Thread.sleep(40 * 1000) //
  }


  "Top Sensor State Reporter" should {
    "correctly output top states for target sensor using DStreams" in {
      setup()

      val ctx: StreamingContext =
        new DstreamTopSensorState().
          beginProcessingInputStream(
            checkpointDirPath, incomingFilesDirPath, outputFile)

      writeRecords()
      verifyResult
      ctx.stop()
    }

    "correctly output top states for target sensor using structured streaming" in {
      import com.lackey.stream.examples.dataset.StreamWriterStrategies._

      setup()

      val query =
        StructuredStreamingTopSensorState.
          processInputStream(doWrites = fileWriter)

      writeRecords()
      verifyResult
      query.stop()
    }
  }


  def writeStringToFile(filePath: String,
                        content: String): Unit = {
    new PrintWriter(filePath) {
      write(content);
      close()
    }
  }

  def verifyResult: Assertion = {
    val output: mutable.Seq[String] =
      FileUtils.readLines(new File(outputFile)).asScala
    val strings: mutable.Seq[String] =
      output.map(_.replaceAll(".*sensor states: ", ""))
    val expected =
      "TreeSet(x2)|TreeSet(x1, x2)|TreeSet(x1, x2)|TreeSet(x1)"
    strings.mkString("|") shouldBe expected
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