package io.github.dejarol.azure.search.spark.connector.core.schema

import com.azure.search.documents.indexes.models.SearchField

/**
 * Simple implementation of the [[SearchFieldCreationContext]] for testing purposes. It
 *  - does not exclude any geopoint field
 *  - does not apply any action to a field
 */

object NoOpFieldCreationContext
  extends SearchFieldCreationContext {

  override def excludeFromGeoConversion(fieldPath: String): Boolean = false

  override def maybeApplyActions(searchField: SearchField, fieldPath: String): SearchField = searchField
}
