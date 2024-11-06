package com.github.jarol.azure.search.spark.sql.connector.models

import com.github.jarol.azure.search.spark.sql.connector.{DocumentDeserializer, PropertyDeserializer}

import java.util.{UUID, Map => JMap}

case class PairBean[T](
                        override val id: String,
                        value: Option[T]
                      )
  extends AbstractITDocument(id)

object PairBean {

  /**
   * Create an instance
   * @param value value
   * @tparam T value type
   * @return an instance
   */

  def apply[T](value: T): PairBean[T] = {

    PairBean(
      UUID.randomUUID().toString,
      Some(value)
    )
  }

  /**
   * Create an ad-hoc [[DocumentDeserializer]]
   * @param colName name of the Search field to use for retrieving the value
   * @tparam T value type (should have an implicit [[PropertyDeserializer]] in scope)
   * @return a document deserializer
   */

  def deserializerFor[T: PropertyDeserializer](colName: String): DocumentDeserializer[PairBean[T]] = {

    (document: JMap[String, AnyRef]) => {
      PairBean(
        document.getProperty[String]("id"),
        document.maybeGetProperty[T](colName)
      )
    }
  }
}
