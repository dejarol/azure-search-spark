package io.github.dejarol.azure.search.spark.connector.core.schema

import com.azure.search.documents.indexes.models.SearchField

/**
 * Data structure for handling a collection of [[com.azure.search.documents.indexes.models.SearchField]]
 * using a syntax similar to the one used by [[org.apache.spark.sql.types.StructType]]
 * @param searchFields collection of Search fields
 */

case class SearchIndexSchema(private val searchFields: Seq[SearchField]) {

  // Fields map for fast field access
  private val lowercaseFieldsMap = searchFields.map {
    field => (field.getName.toLowerCase, field)
  }.toMap

  /**
   * Gets a field by name (comparison is case-insensitive)
   * @param name field name
   * @return a field value if a field with this name exists (comparison is case-insensitive)
   */

  def get(name: String): Option[SearchField] = lowercaseFieldsMap.get(name.toLowerCase)

}
