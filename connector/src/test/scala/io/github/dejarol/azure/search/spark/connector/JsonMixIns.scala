package io.github.dejarol.azure.search.spark.connector

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.github.dejarol.azure.search.spark.connector.core.utils.Reflection

import scala.reflect.ClassTag

/**
 * Trait to mix in suites that deal with JSON
 */

trait JsonMixIns {

  protected final lazy val objMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

  /**
   * Deserializes a JSON string into an object of type T.
   * @param json The JSON string to be deserialized.
   * @tparam T The type of object to deserialize the JSON into. Must have a ClassTag.
   * @return An instance of type T, representing the deserialized JSON object.
   */

  protected final def readValueAs[T: ClassTag](json: String): T = objMapper.readValue(json, Reflection.classFromClassTag[T])

  /**
   * Serialize an object to a JSON string
   * @param value value to serialize
   * @tparam T value type
   * @return a JSON string representation of the value
   */

  protected final def writeValueAs[T](value: T): String = objMapper.writeValueAsString(value)
}
