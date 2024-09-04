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

  protected final def createSearchField(name: String, `type`: SearchFieldDataType): SearchField = new SearchField(name, `type`)

  /**
   * Create a collection type using given inner type
   * @param `type` inner collection type
   * @return a search collection type
   */

  protected final def createCollectionType(`type`: SearchFieldDataType): SearchFieldDataType = SearchFieldDataType.collection(`type`)

  /**
   * Create a collection field using given name and inner type
   * @param name field name
   * @param `type` collection inner type
   * @return a search collection field
   */

  protected final def createCollectionField(name: String, `type`: SearchFieldDataType): SearchField = {

    createSearchField(
      name,
      createCollectionType(`type`)
    )
  }

  /**
   * Create a complex field
   * @param name field name
   * @param fields subFields
   * @return a complex Search field
   */

  protected final def createComplexField(name: String, fields: Seq[SearchField]): SearchField = {

    createSearchField(name, SearchFieldDataType.COMPLEX)
      .setFields(
        JavaScalaConverters.seqToList(fields)
      )
  }
}
