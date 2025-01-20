package io.github.jarol.azure.search.spark.sql.connector.models

/**
 * Parent class for documents used for integration testing
 * @param id document id
 */

abstract class AbstractITDocument(val id: String)
  extends ITDocument {
}
