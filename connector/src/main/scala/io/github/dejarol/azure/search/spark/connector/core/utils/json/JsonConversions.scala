package io.github.dejarol.azure.search.spark.connector.core.utils.json

import com.azure.search.documents.indexes.models.LexicalAnalyzerName
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.core.utils.JavaCollections

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
    override def typeDescription(): String = "string"
    override def isDefinedAt(node: JsonNode): JBoolean = node.isTextual
    override def apply(node: JsonNode): String = node.asText
  }

  /**
   * Conversion for int values
   * @since 0.10.2
   */

  case object IntConversion extends JsonConversion[Int] {
    override def typeDescription(): String = "int"
    override def isDefinedAt(node: JsonNode): JBoolean = node.isInt
    override def apply(node: JsonNode): Int = node.asInt
  }

  /**
   * Conversion for boolean values
   */

  case object BooleanConversion extends JsonConversion[Boolean] {
    override def typeDescription(): String = "boolean"
    override def isDefinedAt(node: JsonNode): JBoolean = node.isBoolean
    override def apply(node: JsonNode): Boolean = node.asBoolean
  }

  /**
   * Conversion for [[com.azure.search.documents.indexes.models.LexicalAnalyzerName]]
   */

  case object LexicalAnalyzerNameConversion
    extends JsonConversion[LexicalAnalyzerName] {

    override def typeDescription(): String = classOf[LexicalAnalyzerName].getName

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

  /**
   * Conversion for array values
   * @param innerConversion conversion for array elements
   * @tparam T type of array elements
   * @since 0.10.2
   */

  private case class ArrayConversion[T](private val innerConversion: JsonConversion[T])
    extends JsonConversion[Seq[T]] {

    override def typeDescription(): String = s"array(${innerConversion.typeDescription()})"

    override def isDefinedAt(node: JsonNode): JBoolean = {

      if (node.isArray) {
        val arrayNode = node.asInstanceOf[ArrayNode]
        JavaScalaConverters.listToSeq(
          JavaCollections.iteratorToList(arrayNode.iterator())
        ).forall {
          innerConversion.isDefinedAt
        }
      } else {
        false
      }
    }

    override def apply(node: JsonNode): Seq[T] = {

      JavaScalaConverters.listToSeq(
        JavaCollections.iteratorToList(node.iterator())
      ).map {
        innerConversion.apply
      }
    }
  }

  /**
   * Create a conversion for arrays
   * @param conversion conversion to apply on inner values
   * @tparam T inner array type
   * @return a conversion for arrays
   * @since 0.10.2
   */

  final def forArrayOf[T](conversion: JsonConversion[T]): JsonConversion[Seq[T]] = ArrayConversion(conversion)
}