package io.github.jarol.azure.search.spark.connector.models

import java.util.{UUID, Map => JMap}

/**
 * Simple model for documents to use for integration testing
 * @param id document id
 * @param value value
 * @tparam T value type
 */

case class PairBean[T](
                        override val id: String,
                        value: Option[T]
                      )
  extends AbstractITDocument(id)

object PairBean {

  /**
   * Create an instance with a random id and a value
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
   * Create an instance with just a random id
   * @tparam T value type
   * @return an instance with just a random id
   */

  def empty[T]: PairBean[T] = {

    PairBean[T](
      UUID.randomUUID().toString,
      None
    )
  }

  /**
   * Create an ad-hoc [[DocumentSerializer]]
   * @tparam T value type (should have an implicit [[PropertySerializer]] in scope)
   * @return a document serializer
   */

  def serializerFor[T: PropertySerializer]: DocumentSerializer[PairBean[T]] = {

    new ITDocumentSerializer[PairBean[T]] {
      override protected def extend(document: PairBean[T], map: JMap[String, AnyRef]): JMap[String, AnyRef] = {
        map.maybeAddProperty[T]("value", document.value)
      }
    }
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
