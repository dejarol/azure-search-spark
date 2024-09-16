package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.SearchFieldDataType

import scala.util.matching.Regex

/**
 * Set of utility methods for a [[SearchFieldDataType]]
 * @param input search data type
 */

class SearchFieldTypeOperations(override protected val input: SearchFieldDataType)
  extends DataTypeOperations[SearchFieldDataType](input, "Search") {

  final def isString: Boolean = input.equals(SearchFieldDataType.STRING)

  /**
   * Returns true if refers to a Search numeric type, i.e.
   *  - int32
   *  - int64
   *  - double
   *  - single
   * @return true for numeric types
   */

  final def isNumeric: Boolean = SearchFieldTypeOperations.NUMERIC_TYPES.contains(input)

  final def isBoolean: Boolean = input.equals(SearchFieldDataType.BOOLEAN)

  final def isDateTime: Boolean = input.equals(SearchFieldDataType.DATE_TIME_OFFSET)

  final def isCollection: Boolean = {

    SearchFieldTypeOperations.COLLECTION_PATTERN
      .findFirstMatchIn(input.toString)
      .isDefined
  }

  final def safeCollectionInnerType: Option[SearchFieldDataType] = {

   SearchFieldTypeOperations.COLLECTION_PATTERN
      .findFirstMatchIn(input.toString)
      .map {
        regexMatch =>
          SearchFieldDataType.fromString(regexMatch.group(1))
      }
  }

  /**
   * Return true is this type is complex
   * @return true for complex types
   */

  final def isComplex: Boolean = input.equals(SearchFieldDataType.COMPLEX)

  /**
   * Evaluate if this search field type is [[SearchFieldDataType.GEOGRAPHY_POINT]]
   * @return true if the search type is a geo point
   */

  final def isGeoPoint: Boolean = input.equals(SearchFieldDataType.GEOGRAPHY_POINT)

  /**
   * Safely extract the inner type of this instance (if it's collection)
   * @return a non-empty value this instance refers to a collection type
   */
}

private object SearchFieldTypeOperations {

  private val COLLECTION_PATTERN: Regex = "^Collection\\(([\\w.]+)\\)$".r

  private val NUMERIC_TYPES: Set[SearchFieldDataType] = Set(
    SearchFieldDataType.INT32,
    SearchFieldDataType.INT64,
    SearchFieldDataType.DOUBLE,
    SearchFieldDataType.SINGLE
  )

  protected[schema] val ATOMIC_TYPES: Set[SearchFieldDataType] = Set(
    SearchFieldDataType.STRING,
    SearchFieldDataType.BOOLEAN,
    SearchFieldDataType.DATE_TIME_OFFSET
  ) ++ NUMERIC_TYPES
}
