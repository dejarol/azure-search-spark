package io.github.dejarol.azure.search.spark.connector.models

import java.util.{Map => JMap}

/**
 * Simple bean for integration tests that involve collection data types
 * @param id document id
 * @param array array
 * @tparam T array type
 */

case class CollectionBean[T](
                              override val id: String,
                              array: Option[Seq[T]]
                            ) extends AbstractITDocument(id) {
}

object CollectionBean {

  /**
   * Create a serializer for given type
   * @tparam T array type (should have an implicit [[PropertySerializer]] in scope)
   * @return a document serializer
   */

  def serializerFor[T: PropertySerializer]: ITDocumentSerializer[CollectionBean[T]] = {

    (document: CollectionBean[T], map: JMap[String, AnyRef]) => {
      map.maybeAddArray[T]("array", document.array)
    }
  }

  /**
   * Create a deserializer for given type
   * @tparam T array type (should have an implicit [[PropertyDeserializer]] in scope)
   * @return a document deserializer
   */

  def deserializerFor[T: PropertyDeserializer]: DocumentDeserializer[CollectionBean[T]] = {

    (document: JMap[String, AnyRef]) => {
      CollectionBean(
        document.getProperty[String]("id"),
        document.maybeGetArray[T]("array")
      )
    }
  }
}
