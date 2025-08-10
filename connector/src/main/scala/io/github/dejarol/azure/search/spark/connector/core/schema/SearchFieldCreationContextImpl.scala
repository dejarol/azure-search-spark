package io.github.dejarol.azure.search.spark.connector.core.schema

import com.azure.search.documents.indexes.models.SearchField

case class SearchFieldCreationContextImpl(
                                           private val excludedFromGeoConversion: Option[Seq[String]],
                                           private val searchFieldActions: Map[String, SearchFieldAction]
                                         ) extends SearchFieldCreationContext {

  override def shouldBeExcludedFromGeoConversion(fieldPath: String): Boolean = {

    excludedFromGeoConversion.exists {
      list => list.exists {
        _.equalsIgnoreCase(fieldPath)
      }
    }
  }

  override def maybeApplyActions(searchField: SearchField, fieldPath: String): SearchField = {

    searchFieldActions.collectFirst {
      case (k, v) if k.equalsIgnoreCase(fieldPath) => v
    }.map {
      _.apply(searchField)
    }.getOrElse(searchField)
  }
}
