package com.lackey.stream.examples.dstream

import java.io.{File, PrintWriter}

import org.apache.commons.io.FileUtils
import org.apache.spark.streaming.StreamingContext
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

class DstreamBasedContinuousTopSensorStateReporterSpec extends WordSpec with Matchers {

  val checkpointDirPath = "/tmp/spark"
  val incomingFilesDirPath = "/tmp/spark.in"
  val outputFile = "/tmp/sensor.state.report"

  val t2_input_path = s"$incomingFilesDirPath/t2_probe_x2_2"
  val t7_input_path = s"$incomingFilesDirPath/t7_probe_x2_1"
  val t12_input_path = s"$incomingFilesDirPath/t12_probe_x1_2"

  def now = java.time.LocalDateTime.now().toString()

  val t2_probe_x2_2 =
    s"""$now,probe,oakland,x1,x2
       |$now,f,freemont,x3,x4
       |$now,probe,freemont,x2,x4
    """.stripMargin

  val t7_probe_x2_1 =
    s"""$now,probe,oakland,x2
       |$now,m,freemont,x2,x3
    """.stripMargin


  val t12_probe_x1_2 =
    s"""$now,probe,oakland,x1
       |$now,m,freemont,x9
       |$now,probe,oakland,x1
    """.stripMargin

  def writeStringToFile(filePath: String, content: String): Unit = {
    new PrintWriter(filePath) {
      write(content); close()
    }
  }


  "ContinuousTopSensorStateReporter" should {

    def verifyResult = {
      val output: mutable.Seq[String] = FileUtils.readLines(new File(outputFile)).asScala
      val strings: mutable.Seq[String] = output.map(_.replaceAll(".*sensor states: ", ""))
      val expected = "Set(x2)|Set(x1, x2)|Set(x1, x2)|Set(x1)"
      strings.mkString("|") shouldBe expected
    }

    def writeRecords(): Unit = {
      Thread.sleep(2 * 1000) //
      writeStringToFile(t2_input_path, t2_probe_x2_2)

      Thread.sleep(5 * 1000) //
      writeStringToFile(t7_input_path, t7_probe_x2_1)

      Thread.sleep(5 * 1000) //
      writeStringToFile(t12_input_path, t12_probe_x1_2)

      Thread.sleep(40 * 1000) //
    }

    "correctly output top states for target sensor" in {
      setup()

      val ctx: StreamingContext =
        new DstreamBasedContinuousTopSensorStateReporter().
          beginProcessingInputStream(checkpointDirPath, incomingFilesDirPath, outputFile)

      writeRecords()
      verifyResult
      ctx.stop()
    }
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