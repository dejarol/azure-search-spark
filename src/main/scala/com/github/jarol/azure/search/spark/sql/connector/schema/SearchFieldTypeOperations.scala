package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException

import scala.util.matching.Regex

/**
 * A set of utility methods for a [[SearchFieldDataType]]
 * @param searchType search data type
 */

class SearchFieldTypeOperations(private val searchType: SearchFieldDataType) {

  /**
   * Return tru if this type is atomic (i.e. string, number, boolean or date)
   * @return true for atomic types
   */

  def isAtomic: Boolean = SearchFieldTypeOperations.ATOMIC_TYPES.contains(searchType)

  /**
   * Return true if this type is a collection
   * @return true for collection type
   */

  def isCollection: Boolean = {

    SearchFieldTypeOperations.COLLECTION_PATTERN
      .findFirstMatchIn(searchType.toString)
      .isDefined
  }

  /**
   * Return true is this type is complex
   * @return true for complex types
   */

  def isComplex: Boolean = searchType.equals(SearchFieldDataType.COMPLEX)

  /**
   * Evaluate if this search field type is [[SearchFieldDataType.GEOGRAPHY_POINT]]
   * @return true if the search type is a geo point
   */

  def isGeoPoint: Boolean = searchType.equals(SearchFieldDataType.GEOGRAPHY_POINT)

  /**
   * Safely extract the inner type of this instance (if it's collection)
   * @return a non-empty value this instance refers to a collection type
   */

  def safelyExtractCollectionType: Option[SearchFieldDataType] = {

   SearchFieldTypeOperations.COLLECTION_PATTERN
      .findFirstMatchIn(searchType.toString)
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
      case None => throw new AzureSparkException(f"Search type $searchType is not a collection")
    }
  }
}

private object SearchFieldTypeOperations {

  private val COLLECTION_PATTERN: Regex = "^Collection\\(([\\w.]+)\\)$".r

  protected[schema] val ATOMIC_TYPES: Set[SearchFieldDataType] = Set(
    SearchFieldDataType.STRING,
    SearchFieldDataType.INT32,
    SearchFieldDataType.INT64,
    SearchFieldDataType.DOUBLE,
    SearchFieldDataType.SINGLE,
    SearchFieldDataType.BOOLEAN,
    SearchFieldDataType.DATE_TIME_OFFSET
  )
}
