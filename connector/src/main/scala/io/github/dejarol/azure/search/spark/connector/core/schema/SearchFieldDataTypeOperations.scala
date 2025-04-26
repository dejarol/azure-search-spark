package io.github.dejarol.azure.search.spark.connector.core.schema

import com.azure.search.documents.indexes.models.SearchFieldDataType
import io.github.dejarol.azure.search.spark.connector.core.EntityDescription

import scala.util.matching.Regex

/**
 * Set of utility methods for a [[com.azure.search.documents.indexes.models.SearchFieldDataType]]
 * @param input search data type
 */

class SearchFieldDataTypeOperations(private val input: SearchFieldDataType)
  extends DataTypeOperations[SearchFieldDataType]
    with EntityDescription {

  override def description: String = s"Search type ${input.toString}"

  override final def isString: Boolean = input.equals(SearchFieldDataType.STRING)

  /**
   * Returns true if refers to a Search numeric type, i.e.
   *  - int32
   *  - int64
   *  - double
   * @return true for numeric types
   */

  override final def isNumeric: Boolean = {
    
    input match {
      case SearchFieldDataType.INT32 | SearchFieldDataType.INT64 | SearchFieldDataType.DOUBLE => true
      case _ => false
    }
  }

  override final def isBoolean: Boolean = input.equals(SearchFieldDataType.BOOLEAN)

  override final def isDateTime: Boolean = input.equals(SearchFieldDataType.DATE_TIME_OFFSET)

  /**
   * Compares this Search type with a custom pattern for detecting if it's a collection.
   * It returns a non-empty match whose first match is the collection subtype if this type is a collection
   * @return a non-empty regex match in case of a collection type
   */

  private def maybeMatchOfCollectionPattern: Option[Regex.Match] = {

    SearchFieldDataTypeOperations.COLLECTION_PATTERN
      .findFirstMatchIn(input.toString)
  }

  final def isCollection: Boolean = maybeMatchOfCollectionPattern.isDefined

  final def safeCollectionInnerType: Option[SearchFieldDataType] = {

    maybeMatchOfCollectionPattern.map {
      regexMatch =>
        SearchFieldDataType.fromString(
          regexMatch.group(1)
        )
    }
  }

  /**
   * Return true is this type is complex
   * @return true for complex types
   */

  final def isComplex: Boolean = input.equals(SearchFieldDataType.COMPLEX)

  /**
   * Evaluate if this search field type is [[com.azure.search.documents.indexes.models.SearchFieldDataType.GEOGRAPHY_POINT]]
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

private object SearchFieldDataTypeOperations {

  private val COLLECTION_PATTERN: Regex = "^Collection\\(([\\w.]+)\\)$".r
}
