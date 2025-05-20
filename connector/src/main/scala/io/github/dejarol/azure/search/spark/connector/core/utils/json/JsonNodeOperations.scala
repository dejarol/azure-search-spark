package io.github.dejarol.azure.search.spark.connector.core.utils.json

import com.fasterxml.jackson.databind.JsonNode

import scala.language.implicitConversions

/**
 * Wrapper class for operating on jackson's API JSON nodes in a Scala-friendly way
 * @param node a node instance
 */

class JsonNodeOperations(private val node: JsonNode) {

  /**
   * Safely converts this node to the specified type, if possible.
   * The return value will be defined only if
   *  - the key is present
   *  - the value can be converted to the target type
   * @param key key to convert
   * @tparam T target value type
   * @return an optional return value
   */

  final def safelyGetAs[T: JsonConversion](key: String): Option[T] = {

    node.getNodeType
    if (node.has(key)) {
      val conversion = implicitly[JsonConversion[T]]
      val nodeToConvert = node.get(key)
      if (conversion.isDefinedAt(nodeToConvert)) {
        Some(conversion.apply(nodeToConvert))
      } else {
        None
      }
    } else {
      None
    }
  }

  /**
   * Unsafely gets a value from this node
   * @param key key to convert
   * @tparam T target value type
   * @return an instance of the target type
   */

  @throws[IllegalArgumentException]
  final def getAs[T: JsonConversion](key: String): T = {

    safelyGetAs(key) match {
      case Some(value) => value
      case None =>

        val typeDescription = implicitly[JsonConversion[T]].typeDescription
        throw new IllegalArgumentException(
          s"JSON key $key not found or not deserializable as a $typeDescription"
      )
    }
  }
}

object JsonNodeOperations {

  /**
   * Implicit conversion from a node to its operations counterpart
   * @param node a JSON node (from jackson API)
   * @return an instance of [[JsonNodeOperations]]
   */

  implicit def toJsonNodeOperations(node: JsonNode): JsonNodeOperations = new JsonNodeOperations(node)
}
