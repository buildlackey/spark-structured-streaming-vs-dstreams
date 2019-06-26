package org.apache.spark.sql.execution.datasources.text

import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CompressionCodecs}

/**
 * Options for the Text data source.
 */
class PestOptions(@transient private val parameters: CaseInsensitiveMap[String])
  extends Serializable {

  import TextOptions._

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  /**
   * Compression codec to use.
   */
  val compressionCodec = parameters.get(COMPRESSION).map(CompressionCodecs.getCodecClassName)

  /**
   * wholetext - If true, read a file as a single row and not split by "\n".
   */
  val wholeText = parameters.getOrElse(WHOLETEXT, "false").toBoolean

}

private[text] object TextOptions {
  val COMPRESSION = "compression"
  val WHOLETEXT = "wholetext"
}

