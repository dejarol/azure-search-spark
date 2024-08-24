package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.AtomicInferSchemaRules

import scala.util.matching.Regex

/**
 * Wrapper for adding utility methods to a [[SearchFieldDataType]]
 * @param `type` search data type
 */

class SearchFieldTypeWrapper(private val `type`: SearchFieldDataType) {

  /**
   * Return tru if this type is atomic (i.e. string, number, boolean or date)
   * @return true for atomic types
   */

  def isAtomic: Boolean = AtomicInferSchemaRules.existsRuleForType(`type`)

  /**
   * Return true if this type is a collection
   * @return true for collection type
   */

  def isCollection: Boolean = {

    SearchFieldTypeWrapper.COLLECTION_PATTERN
      .findFirstMatchIn(`type`.toString)
      .isDefined
  }

  /**
   * Return true is this type is complex
   * @return true for complex types
   */

  def isComplex: Boolean = `type`.equals(SearchFieldDataType.COMPLEX)

  /**
   * Evaluate if this search field type is [[SearchFieldDataType.GEOGRAPHY_POINT]]
   * @return true if the search type is a geo point
   */

  def isGeoPoint: Boolean = `type`.equals(SearchFieldDataType.GEOGRAPHY_POINT)

  /**
   * Safely extract the inner type of this instance (if it's collection)
   * @return a non-empty value this instance refers to a collection type
   */

  def safelyExtractCollectionType: Option[SearchFieldDataType] = {

   SearchFieldTypeWrapper.COLLECTION_PATTERN
      .findFirstMatchIn(`type`.toString)
      .map {
        regexMatch =>
          SearchFieldDataType.fromString(regexMatch.group(1))
      }
  }

  /**
   * Unsafely extract the inner type of this instance
   * @throws AzureSparkException if this instance does not refer to a collection type
   * @return the inner collection type
   */

  @throws[AzureSparkException]
  def unsafelyExtractCollectionType: SearchFieldDataType = {

    safelyExtractCollectionType match {
      case Some(value) => value
      case None => throw new AzureSparkException(
        f"Illegal state (a collection type is expected but no inner type could be found)"
      )
    }
  }
}

private object SearchFieldTypeWrapper {

  private val COLLECTION_PATTERN: Regex = "^Collection\\(([\\w.]+)\\)$".r
}
