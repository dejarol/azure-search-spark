package com.github.jarol.azure.search.spark.sql.connector

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}

/**
 * Trait to mix-in for suites that have to deal with [[SearchField]](s)
 */

trait SearchFieldFactory {

  /**
   * Create a simple search field
   * @param name field name
   * @param `type` field type
   * @return a search field
   */

  protected final def createField(name: String, `type`: SearchFieldDataType): SearchField = new SearchField(name, `type`)
}
