package io.github.dejarol.azure.search.spark.connector.core.schema

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters

import java.util.Objects
import scala.util.matching.Regex

case class SearchFieldOperations(private val searchField: SearchField)
  extends FieldOperations[SearchFieldDataType, SearchField]
    with FieldDescriptor {

  import SearchFieldOperations._

  private val searchFieldDataType: SearchFieldDataType = searchField.getType

  override def isString: Boolean = searchFieldDataType.equals(SearchFieldDataType.STRING)

  /**
   * Returns true if refers to a Search numeric type, i.e.
   *  - int32
   *  - int64
   *  - double
   * @return true for numeric types
   */

  final def isNumeric: Boolean = {

    searchFieldDataType match {
      case SearchFieldDataType.INT32 | SearchFieldDataType.INT64 | SearchFieldDataType.DOUBLE => true
      case _ => false
    }
  }

  final def isBoolean: Boolean = searchFieldDataType.equals(SearchFieldDataType.BOOLEAN)

  final def isDateTime: Boolean = searchFieldDataType.equals(SearchFieldDataType.DATE_TIME_OFFSET)

  /**
   * Compares this Search type with a custom pattern for detecting if it's a collection.
   * It returns a non-empty match whose first match is the collection subtype if this type is a collection
   * @return a non-empty regex match in case of a collection type
   */

  private def maybeMatchOfCollectionPattern: Option[Regex.Match] = COLLECTION_PATTERN.findFirstMatchIn(searchFieldDataType.toString)

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

  final def isComplex: Boolean = searchFieldDataType.equals(SearchFieldDataType.COMPLEX)

  override def safeSubFields: Option[Seq[SearchField]] = {

    val subFields = searchField.getFields
    if (Objects.isNull(subFields) || subFields.isEmpty) {
      None
    } else {
      Some(
        JavaScalaConverters.listToSeq(
          subFields
        )
      )
    }
  }

  /**
   * Evaluate if this search field type is [[com.azure.search.documents.indexes.models.SearchFieldDataType.GEOGRAPHY_POINT]]
   * @return true if the search type is a geo point
   */

  final def isGeoPoint: Boolean = searchFieldDataType.equals(SearchFieldDataType.GEOGRAPHY_POINT)

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

  override def name(): String = searchField.getName

  override def `type`(): String = "Search"

  override def dataTypeDescription(): String = searchFieldDataType.toString

  /**
   * Apply a collection of actions on this field
   * @param actions actions to apply
   * @return this field transformed by the many actions provided
   */

  final def applyActions(actions: SearchFieldAction*): SearchField = {

    actions.foldLeft(searchField) {
      case (field, action) =>
        action.apply(field)
    }
  }

  /**
   * Evaluate if a feature is enabled on this field
   * @param feature feature
   * @return true for enabled features
   */

  final def isEnabledFor(feature: SearchFieldFeature): Boolean = feature.isEnabledOnField(searchField)
}

object SearchFieldOperations {

  private val COLLECTION_PATTERN: Regex = "^Collection\\(([\\w.]+)\\)$".r
}