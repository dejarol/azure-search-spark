package com.github.jarol.azure.search.spark.sql.connector

import com.azure.search.documents.indexes.models.SearchFieldDataType

import scala.language.implicitConversions

package object schema {

  implicit def toSearchFieldWrapper(`type`: SearchFieldDataType): SearchFieldTypeWrapper = new SearchFieldTypeWrapper(`type`)

}

