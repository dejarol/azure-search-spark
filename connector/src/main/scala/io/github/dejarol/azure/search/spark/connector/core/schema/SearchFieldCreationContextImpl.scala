package io.github.dejarol.azure.search.spark.connector.core.schema

import com.azure.search.documents.indexes.models.SearchField

/**
 * Default implementation of the field creation context. It collects write options defined by the user
 * @param excludedFromGeoConversion fields mentioned in <code>excludeFromGeoConversion</code> write config
 * @param searchFieldActions actions retrieved by deserializing all write options prefixed by <code>fieldOptions.</code>
 * @since 0.12.0
 */

case class SearchFieldCreationContextImpl(
                                           private val excludedFromGeoConversion: Option[Seq[String]],
                                           private val searchFieldActions: Map[String, SearchFieldAction]
                                         ) extends SearchFieldCreationContext {

  /**
   * Decide whether the candidate field should be excluded, evaluating the presence of the candidate field path
   * within the collection of user-defined field paths defined by write config <code>excludeFromGeoConversion</code>
   * @param fieldPath path of the candidate field
   * @return true for fields to be excluded
   */

  override def excludeFromGeoConversion(fieldPath: String): Boolean = {

    excludedFromGeoConversion.exists {
      list => list.exists {
        _.equalsIgnoreCase(fieldPath)
      }
    }
  }

  /**
   * Transform the input field, if there's a key equal to the given field path
   * @param searchField candidate field
   * @param fieldPath   field path (i.e. the field name for a top-level field, the field path for nested fields)
   * @return the input field, potentially transformed
   */

  override def maybeApplyActions(searchField: SearchField, fieldPath: String): SearchField = {

    searchFieldActions.collectFirst {
      case (k, v) if k.equalsIgnoreCase(fieldPath) => v
    }.map {
      _.apply(searchField)
    }.getOrElse(searchField)
  }
}
