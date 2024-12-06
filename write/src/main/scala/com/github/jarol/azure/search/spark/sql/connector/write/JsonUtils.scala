package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.json.implementation.DefaultJsonReader
import com.azure.json.{JsonOptions, JsonReader}
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters

import scala.util.Try
import java.util.{List => JList}

/**
 * Collection of utility methods for dealing with json serialization/deserialization
 */

object JsonUtils {

  /**
   * Deserialize a json to an Azure Search REST API model.
   * <br>
   * This operation is unsafe, i.e. no exception handling is provided
   * @param json json string
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
   * Deserialize a json to an Azure Search REST API model.
   * <br>
   * This operation is unsafe, i.e. no exception handling is provided
   * @param json json
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
   * Deserialize a json string to an array of Azure Search REST API models
   * <br>
   * This operation is unsafe, i.e. no exception handling is provided
   * @param json json string
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
   * Safely deserialize a json string to an Azure Search REST API model
   * @param json json string
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
   * Safely deserialize a json string to an array of Azure Search REST API models
   * @param json json string
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
