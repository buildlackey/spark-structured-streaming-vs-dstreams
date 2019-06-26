package com.lackey.stream.examples

import java.io.{FileWriter, PrintWriter}
import java.util.Calendar

object FileUtils {
  def writeStringToFile(outputPath: String,
                        content: String): Unit = {
    val fileWriter = new FileWriter(outputPath, true)
    val printWriter = new PrintWriter(fileWriter)
    val timestampedOutput = s"${Calendar.getInstance.getTime}: $content"
    printWriter.println(timestampedOutput)
    fileWriter.close()
  }
}
