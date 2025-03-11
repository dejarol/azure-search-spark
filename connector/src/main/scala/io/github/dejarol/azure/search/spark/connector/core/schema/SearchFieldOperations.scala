package io.github.dejarol.azure.search.spark.connector.core.schema

import com.azure.search.documents.indexes.models.SearchField
import org.apache.spark.sql.types.StructField

/**
 * Set of utilities for dealing with [[com.azure.search.documents.indexes.models.SearchField]](s)
 * @param field search field
 */

class SearchFieldOperations(private val field: SearchField) {

  /**
   * Evaluates if this field has the same name with respect to given Spark field
   * @param sparkField spark field
   * @return true for same names (case-insensitive)
   */

  final def sameNameOf(sparkField: StructField): Boolean = field.getName.equalsIgnoreCase(sparkField.name)

  /**
   * Evaluate if a feature is enabled on this field
   * @param feature feature
   * @return true for enabled features
   */

  final def isEnabledFor(feature: SearchFieldFeature): Boolean = feature.isEnabledOnField(field)

  /**
   * Apply a collection of actions on this field
   * @param actions actions to apply
   * @return this field transformed by the many actions provided
   */

  final def applyActions(actions: SearchFieldAction*): SearchField = {

    actions.foldLeft(field) {
      case (field, action) =>
        action.apply(field)
    }
  }
}
