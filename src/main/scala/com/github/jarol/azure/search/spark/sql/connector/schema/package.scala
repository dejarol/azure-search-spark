package com.github.jarol.azure.search.spark.sql.connector

import com.azure.search.documents.indexes.models.SearchFieldDataType

import scala.language.implicitConversions

package object schema {

  /**
   * Create an instance of [[SearchFieldTypeOperations]] from a Search field type
   * @param `type` search field type
   * @return an operations instance
   */

  implicit def toSearchTypeOperations(`type`: SearchFieldDataType): SearchFieldTypeOperations = new SearchFieldTypeOperations(`type`)

}

