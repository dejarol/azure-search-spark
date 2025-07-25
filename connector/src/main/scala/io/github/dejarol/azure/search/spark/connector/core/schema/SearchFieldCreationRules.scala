package io.github.dejarol.azure.search.spark.connector.core.schema

import com.azure.search.documents.indexes.models.SearchField

trait SearchFieldCreationRules {

  def shouldBeExcludedFromGeoConversion(name: String): Boolean

  def maybeApplyActions(searchField: SearchField, fieldPath: String): SearchField
}