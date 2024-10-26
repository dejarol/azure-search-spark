package com.github.jarol.azure.search.spark.sql.connector.models

/**
 * Simple bean for integration tests that involve collection data types
 * @param id document id
 * @param array array
 * @tparam T array type
 */

case class CollectionBean[T](
                              override val id: String,
                              array: Option[Seq[T]]
                            ) extends ITDocument(id)
