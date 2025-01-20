package io.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchIndex

/**
 * Wrapper class for adding functionalities to a Search index
 * @param index input Search index definition
 */

class SearchIndexOperations(private val index: SearchIndex) {

  /**
   * Apply some actions to this index
   * @param actions actions to apply
   * @return a transformed version of this index
   */

  final def applyActions(actions: SearchIndexAction*): SearchIndex = {

    actions.foldLeft(index) {
      case (index, action) =>
        action.apply(index)
    }
  }
}
