package io.github.dejarol.azure.search.spark.connector.core.utils

import com.azure.json.implementation.DefaultJsonReader
import com.azure.json.{JsonOptions, JsonReader}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters

import java.util.{List => JList}
import scala.reflect.ClassTag
import scala.util.Try

/**
 * Collection of utility methods for dealing with JSON serialization/deserialization
 */

object Json {

  private lazy val DEFAULT_OBJECT_MAPPER = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

  /**
   * Safely deserialize a JSON string to a model class using Jackson
   * @param json JSON string
   * @tparam T target type (should have an implicit ClassTag)
   * @return either the exception raised by the deserialization function or an instance of the target type
   */

  final def safelyReadAsModelUsingJackson[T: ClassTag](json: String): Either[Throwable, T] = {

    Try {
      val tClass = Reflection.classFromClassTag[T]
      DEFAULT_OBJECT_MAPPER.readValue[T](json, tClass)
    }.toEither
  }

  /**
   * Deserializes a JSON string into a collection of objects using Jackson.
   * @param json json string
   * @tparam T type of objects in the resulting collection. Must have a ClassTag.
   * @return a collection containing the deserialized objects.
   */

  final def readAsCollectionUsingJackson[T: ClassTag](json: String): Seq[T] = {

    val tClass = Reflection.classFromClassTag[T]
    val collectionType = DEFAULT_OBJECT_MAPPER.getTypeFactory.constructCollectionType(classOf[JList[_]], tClass)
    JavaScalaConverters.listToSeq[T](
      DEFAULT_OBJECT_MAPPER.readValue[JList[T]](json, collectionType)
    )
  }

  /**
   * Deserialize a JSON to an Azure Search REST API model.
   * <br>
   * This operation is unsafe, i.e. no exception handling is provided
   * @param json JSON string
   * @param function deserialization function
   * @tparam V target type
   * @return an instance of the target type
   */

  private def withAzJsonStringDo[V](json: String)(function: JsonReader => V): V = {

    function(
      DefaultJsonReader.fromString(
        json,
        new JsonOptions
      )
    )
  }

  /**
   * Deserialize a JSON to an Azure Search REST API model.
   * <br>
   * This operation is unsafe, i.e. no exception handling is provided
   * @param json JSON
   * @param function deserialization function
   * @tparam V target type
   * @return an instance of target type
   */

  final def unsafelyReadAzModel[V](
                                    json: String,
                                    function: JsonReader => V
                                  ): V = {

    withAzJsonStringDo[V](json) {
      function
    }
  }

  /**
   * Deserialize a JSON string to an array of Azure Search REST API models
   * <br>
   * This operation is unsafe, i.e. no exception handling is provided
   * @param json JSON string
   * @param function deserialization function
   * @tparam V array inner type
   * @return either the exception raised by the deserialization function or an array of target type instances
   */

  final def unsafelyReadAzModelArray[V](
                                         json: String,
                                         function: JsonReader => V
                                       ): Seq[V] = {

    val list: JList[V] = withAzJsonStringDo[JList[V]](json) {
      _.readArray[V] {
        (input: JsonReader) => function(input)
      }
    }

    JavaScalaConverters.listToSeq(list)
  }

  /**
   * Safely deserialize a JSON string to an Azure Search REST API model
   * @param json JSON string
   * @param function deserialization function
   * @tparam V target value type
   * @return either the exception raised by the deserialization function or an instance of the target type
   */

  final def safelyReadAzModel[V](
                                  json: String,
                                  function: JsonReader => V
                                ): Either[Throwable, V] = {

    Try {
      unsafelyReadAzModel[V](json, function)
    }.toEither
  }

  /**
   * Safely deserialize a JSON string to an array of Azure Search REST API models
   * @param json JSON string
   * @param function deserialization function
   * @tparam V array inner type
   * @return either the exception raised by the deserialization function or an array of target type instances
   */

  final def safelyReadAzModelArray[V](
                                       json: String,
                                       function: JsonReader => V
                                     ) : Either[Throwable, Seq[V]] = {

    Try {
      unsafelyReadAzModelArray[V](json, function)
    }.toEither
  }
}
