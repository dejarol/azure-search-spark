package io.github.dejarol.azure.search.spark.connector.core.schema

import com.azure.search.documents.indexes.models.SearchField

/**
 * Context for managing the process that, during the write process, sets the
 * Search index fields. Setting the fields involves
 *  - setting the name and datatype
 *  - setting fields metadata (<code>key</code>, <code>filterable</code>, etc ...)
 * @since 0.12.0
 */

trait SearchFieldCreationContext {

  /**
   * Decide whether a field, even though it's a valid geopoint, should be excluded from the natural geopoint
   * conversion strategy defined by this connector. The field path should be the field name for a top-level field, the field
   * path for a nested field
   * @param fieldPath path of the candidate field
   * @return true for fields to be excluded
   */

  def excludeFromGeoConversion(fieldPath: String): Boolean

  /**
   * Transform a given field, if needed, by applying some field actions
   * @param searchField candidate field
   * @param fieldPath field path (i.e. the field name for a top-level field, the field path for nested fields)
   * @return the input field, potentially transformed
   */

  def maybeApplyActions(searchField: SearchField, fieldPath: String): SearchField
}