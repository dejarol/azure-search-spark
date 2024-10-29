package com.github.jarol.azure.search.spark.sql.connector.models

import com.github.jarol.azure.search.spark.sql.connector.ITDocumentSerializer

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

  def serializerFor[T: PropertySerializer]: ITDocumentSerializer[CollectionBean[T]] = {

    (document: CollectionBean[T], map: JMap[String, AnyRef]) => {
      map.maybeAddArray[T]("array", document.array)
    }
  }
}
