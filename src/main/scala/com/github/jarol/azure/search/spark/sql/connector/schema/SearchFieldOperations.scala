package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.SearchField
import org.apache.spark.sql.types.StructField

/**
 * Set of utilities for dealing with [[SearchField]](s)
 * @param field search field
 */

class SearchFieldOperations(private val field: SearchField) {

  /**
   * Evaluates if this field has the same name with respect to given Spark field
   * @param sparkField spark field
   * @return true for same names (case-insensitive)
   */

  final def sameNameOf(sparkField: StructField): Boolean = field.getName.equalsIgnoreCase(sparkField.name)

}
