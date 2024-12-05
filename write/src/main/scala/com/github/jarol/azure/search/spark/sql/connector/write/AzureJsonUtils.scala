package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.json.implementation.DefaultJsonReader
import com.azure.json.{JsonOptions, JsonReader}
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters

import scala.util.Try

object AzureJsonUtils {

  private def withJsonStringSafelyDo[V](json: String)(function: JsonReader => V): Either[Throwable, V] = {

    Try {
      function(
        DefaultJsonReader.fromString(
          json,
          new JsonOptions
        )
      )
    }.toEither
  }

  private[write] def safeReadObject[V](
                                        json: String,
                                        function: JsonReader => V
                                      ): Either[Throwable, V] = {

    withJsonStringSafelyDo(json) {
      _.readObject[V] {
        (input: JsonReader) =>
          function(input)
      }
    }
  }

  private[write] def safeReadArray[V](
                                       json: String,
                                       function: JsonReader => V
                                     ) : Either[Throwable, Seq[V]] = {
    withJsonStringSafelyDo(json) {
      _.readArray[V] {
        (input: JsonReader) =>
          function(input)
      }
    }.map {
      JavaScalaConverters.listToSeq
    }
  }
}
