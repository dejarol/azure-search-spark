package com.github.jarol.azure.search.spark.sql.connector.core.schema

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
   * @return true for numeric types
   */

  final def isNumeric: Boolean = {

    input match {
      case SearchFieldDataType.INT32 | SearchFieldDataType.INT64 | SearchFieldDataType.DOUBLE => true
      case _ => false
    }
  }

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
   * Evaluate if this type is a candidate for partitioning.
   * <br>
   * Only numeric or date time types are candidates
   * @return true for numeric or date time types
   */

  final def isCandidateForPartitioning: Boolean = isDateTime || isNumeric

  /**
   * Evaluate if this type is a candidate for faceting
   * <br>
   * Only string or numeric types are good candidates
   * @return true for string or numeric types
   */

  final def isCandidateForFaceting: Boolean = isString || isNumeric
}

private object SearchFieldTypeOperations {

  private val COLLECTION_PATTERN: Regex = "^Collection\\(([\\w.]+)\\)$".r
}
