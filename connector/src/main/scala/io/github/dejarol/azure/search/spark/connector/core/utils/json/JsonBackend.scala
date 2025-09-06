package io.github.dejarol.azure.search.spark.connector.core.utils.json

import scala.util.Try

/**
 * Trait for defining a JSON deserialization backend
 * @tparam T target deserialization type
 * @since 0.12.0
 */

trait JsonBackend[T] {

  /**
   * Safely deserialize a JSON string. If the deserialization fails, it will return a <code>Left</code>
   * holding the handled exception, otherwise it will return a <code>Right</code> holding the deserialized
   * object
   * @param json raw JSON string
   * @return either a success (the deserialized object) or a failure (the handled exception)
   */

  final def safelyDeserialize(json: String): Either[Throwable, T] = {

    Try {
      deserialize(json)
    }.toEither
  }

  /**
   * Unsafely deserialize a JSON string (no exception handling)
   * @param json raw JSON string
   * @return the deserialized object
   */

  def deserialize(json: String): T
}