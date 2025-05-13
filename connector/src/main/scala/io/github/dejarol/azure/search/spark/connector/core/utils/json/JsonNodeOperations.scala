package io.github.dejarol.azure.search.spark.connector.core.utils.json

import com.fasterxml.jackson.databind.JsonNode

import scala.language.implicitConversions

// TODO: document
class JsonNodeOperations(private val node: JsonNode) {

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

  final def getAs[T: JsonConversion](key: String): T = {

    safelyGetAs(key) match {
      case Some(value) => value
      case None => throw new NoSuchElementException(
        s"JSON key $key not found"
      )
    }
  }
}

object JsonNodeOperations {

  implicit def toJsonNodeOperations(node: JsonNode): JsonNodeOperations = new JsonNodeOperations(node)
}
