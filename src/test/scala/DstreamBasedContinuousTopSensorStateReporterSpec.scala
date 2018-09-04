import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.scalatest.{Matchers, WordSpec}
import java.io.{File, PrintWriter}
import java.util

import org.apache.commons.io.FileUtils
import org.apache.commons.io.FileUtils
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

class DstreamBasedContinuousTopSensorStateReporterSpec extends WordSpec with Matchers {

  val checkpointDirPath = "/tmp/spark"
  val incomingFilesDirPath = "/tmp/spark.in"
  val outputFile = "/tmp/sensor.state.report"

  val t2_input_path = s"$incomingFilesDirPath/t2_probe_x2_2"
  val t7_input_path = s"$incomingFilesDirPath/t7_probe_x2_1"
  val t12_input_path = s"$incomingFilesDirPath/t12_probe_x1_2"


  val t2_probe_x2_2 =
    """
      |probe,oakland,x1,x2
      |f,freemont,x3,x4
      |probe,freemont,x2,x4
    """.stripMargin

  val t7_probe_x2_1 =
    """
      |probe,oakland,x2
      |m,freemont,x2,x3
    """.stripMargin


  val t12_probe_x1_2 =
    """
      |probe,oakland,x1
      |m,freemont,x9
      |probe,oakland,x1
    """.stripMargin

  def writeStringToFile(filePath: String, content: String): Unit = {
    new PrintWriter(filePath) { write(content); close() }
  }


  "ContinuousTopSensorStateReporter" should {
    "correctly output top states for target sensor" in {

      // TODO - factor out all the File creates to top of function

      // Delete and recreate checkpoint and input directories and any old version of output file
      //
      new File(outputFile).delete()
      FileUtils.deleteDirectory(new File(checkpointDirPath))
      FileUtils.deleteDirectory(new File(incomingFilesDirPath))
      new File(checkpointDirPath).mkdirs()
      new File(incomingFilesDirPath).mkdirs()

      val ctx: StreamingContext  =
        new DstreamBasedContinuousTopSensorStateReporter().
          beginProcessingInputStream(checkpointDirPath, incomingFilesDirPath, outputFile)

      Thread.sleep(2 * 1000) //
      writeStringToFile(t2_input_path, t2_probe_x2_2)

      Thread.sleep(5 * 1000) //
      writeStringToFile(t7_input_path, t7_probe_x2_1)

      Thread.sleep(5 * 1000) //
      writeStringToFile(t12_input_path, t12_probe_x1_2)

      Thread.sleep(40 * 1000) //
      ctx.stop()

      val output: mutable.Seq[String] = FileUtils.readLines(new File(outputFile)).asScala
      val strings: mutable.Seq[String] = output.map(_.replaceAll(".*sensor states: ", ""))
      val expectedResult = "Set(x2)|Set(x1, x2)|Set(x1, x2)|Set(x1)"

      strings.mkString("|") shouldBe "Set(x2)|Set(x1, x2)|Set(x1, x2)|Set(x1)"
    }
  }
}