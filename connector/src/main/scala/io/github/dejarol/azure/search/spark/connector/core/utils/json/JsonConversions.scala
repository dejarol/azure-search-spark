package io.github.dejarol.azure.search.spark.connector.core.utils.json

import com.azure.search.documents.indexes.models.LexicalAnalyzerName
import com.fasterxml.jackson.databind.JsonNode

import java.lang.{Boolean => JBoolean}

/**
 * Collection of custom [[io.github.dejarol.azure.search.spark.connector.core.utils.json.JsonConversion]](s),
 * that can be exploited within custom implementations of Jackson-based deserializers
 */

object JsonConversions {

  /**
   * Conversion for string values
   */

  case object StringConversion extends JsonConversion[String] {
    override def isDefinedAt(node: JsonNode): JBoolean = node.isTextual
    override def apply(node: JsonNode): String = node.asText
  }

  /**
   * Conversion for boolean values
   */

  case object BooleanConversion extends JsonConversion[Boolean] {
    override def isDefinedAt(node: JsonNode): JBoolean = node.isBoolean
    override def apply(node: JsonNode): Boolean = node.asBoolean
  }

  /**
   * Conversion for [[com.azure.search.documents.indexes.models.LexicalAnalyzerName]]
   */

  case object LexicalAnalyzerNameConversion
    extends JsonConversion[LexicalAnalyzerName] {
    override def isDefinedAt(node: JsonNode): JBoolean = {

      // Evaluate if the node is a string
      if (node.isTextual) {

        // and then if it's a valid analyzer name
        LexicalAnalyzerName.values().stream().anyMatch(
          (t: LexicalAnalyzerName) =>
            t.toString.equalsIgnoreCase(node.asText())
        )
      } else {
        false
      }
    }

    override def apply(node: JsonNode): LexicalAnalyzerName = {

      LexicalAnalyzerName.fromString(
        node.asText()
      )
    }
  }
}