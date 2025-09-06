package io.github.dejarol.azure.search.spark.connector.core.utils.json

import com.azure.json.{JsonOptions, JsonReader}
import com.azure.json.implementation.DefaultJsonReader
import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import org.json4s.Formats
import org.json4s.jackson.JsonMethods.parse

import java.util.{List => JList}

/**
 * Object holding factory methods for creating [[JsonBackend]] instances
 * @since 0.12.0
 */

object JsonBackends {

  /**
   * Backend implementation for JSON4S
   * @param formats formats used during deserialization
   * @tparam T target deserialization type (must have an implicit <code>Manifest</code> in scope)
   */

  private class Json4SBackend[T: Manifest](private val formats: Formats)
    extends JsonBackend[T] {

    override def deserialize(json: String): T = {

      val manifestOfT = implicitly[Manifest[T]]
      parse(json).extract[T](formats, manifestOfT)
    }
  }

  /**
   * Backend implementation for Azure Search REST API models
   * @param deserializationFunction function to use for deserialization
   * @tparam T target deserialization type
   */

  private class AzureBackend[T](private val deserializationFunction: JsonReader => T)
    extends JsonBackend[T] {

    override def deserialize(json: String): T = {

      deserializationFunction(
        DefaultJsonReader.fromString(
          json,
          new JsonOptions
        )
      )
    }
  }

  /**
   * Create a JSON4S backend for given type, providing a set of formats
   * @param formats formats to use for deserialization
   * @tparam T target deserialization type (must have an implicit <code>Manifest</code> in scope)
   * @return a backend implementation relying on JSON4S
   */

  final def json4s[T: Manifest](formats: Formats): JsonBackend[T] = new Json4SBackend[T](formats)

  /**
   * Create a backend for Azure Search REST API models
   * @param function function to use for deserialization
   * @tparam T target deserialization type
   * @return a backend implementation for deserializing Azure Search REST API models
   */

  final def forAzureModel[T](function: JsonReader => T): JsonBackend[T] = new AzureBackend[T](function)

  /**
   * Create a backend for deserializing arrays of Azure Search REST API models
   * @param elementDeserializationFunction function to use for deserializing a single array element
   * @tparam T array element type
   * @return a backend implementation for deserializing arrays of Azure Search REST API models
   */

  final def forAzureArrayOf[T](elementDeserializationFunction: JsonReader => T): JsonBackend[Seq[T]] = {

    // Create a function for deserializing an array
    val arrayDeserializationFunction: JsonReader => Seq[T] = {
      reader: JsonReader =>

        // Read the Azure JSON array as a Java List, then convert it to a Scala Seq
        val javaList: JList[T] = reader.readArray[T] {
          (input: JsonReader) =>
            elementDeserializationFunction(input)
        }

        JavaScalaConverters.listToSeq(javaList)
    }

    new AzureBackend[Seq[T]](arrayDeserializationFunction)
  }
}
