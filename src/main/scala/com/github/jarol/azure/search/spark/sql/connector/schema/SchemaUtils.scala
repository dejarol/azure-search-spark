package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.types.conversion.{InferSchemaRules, GeoPointRule}
import com.github.jarol.azure.search.spark.sql.connector.{AzureSparkException, JavaScalaConverters}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import scala.util.matching.Regex

/**
 * Utilities for inferring the schema of a SearchIndex
 */

object SchemaUtils {

  private val COLLECTION_PATTERN: Regex = "^Collection\\(([\\w.]+)\\)$".r

  /**
   * Evaluate if a search field type is simple (e.g. string, number, boolean or date)
   * @param searchType search type to test
   * @return true if the search type is simple (e.g. either a string or a number or a boolean or a date)
   */

  protected[schema] def isAtomicType(searchType: SearchFieldDataType): Boolean = InferSchemaRules.existsRuleForType(searchType)

  /**
   * Evaluate if a search field type is complex
   * @param searchType search type to test
   * @return true if the search type is complex
   */

  protected[schema] def isComplexType(searchType: SearchFieldDataType): Boolean = SearchFieldDataType.COMPLEX.equals(searchType)

  /**
   * Evaluate if a search field type is a collection type
   * @param searchType search type to test
   * @return true if the search type is a collection
   */

  protected[schema] def isCollectionType(searchType: SearchFieldDataType): Boolean = {

    COLLECTION_PATTERN
      .findFirstMatchIn(searchType.toString)
      .isDefined
  }

  /**
   * Evaluate if a search field type is a geo point
   * @param searchType search type to test
   * @return true if the search type is a geo point
   */

  protected[schema] def isGeoPoint(searchType: SearchFieldDataType): Boolean = SearchFieldDataType.GEOGRAPHY_POINT.equals(searchType)


  /**
   * Safely extract the inner type of collection type (if given type is a collection)
   * @param searchType search type
   * @return a non-empty value if the input search type is a collection
   */

  protected[schema] def safelyExtractCollectionType(searchType: SearchFieldDataType): Option[SearchFieldDataType] = {

    COLLECTION_PATTERN
      .findFirstMatchIn(searchType.toString)
      .map {
        regexMatch =>
          SearchFieldDataType.fromString(regexMatch.group(1))
    }
  }

  /**
   * Extract the inner type of collection type
   * @param searchType search type
   * @throws AzureSparkException if the given search type is not a collection type
   * @return the inner collection type
   */

  @throws[AzureSparkException]
  protected[schema] def unsafelyExtractCollectionType(searchType: SearchFieldDataType): SearchFieldDataType = {

    safelyExtractCollectionType(searchType) match {
      case Some(value) => value
      case None => throw new AzureSparkException(
        f"Illegal state (a collection type is expected but no inner type could be found)"
      )
    }
  }

  /**
   * Return the Spark equivalent [[DataType]] for a search field
   * @param searchField a search field
   * @throws AzureSparkException if given search type is not supported
   * @return the equivalent Spark data type for given search field
   */

  @throws[AzureSparkException]
  def sparkDataTypeOf(searchField: SearchField): DataType = {

    val searchType = searchField.getType
    if (isAtomicType(searchType)) {
      InferSchemaRules
        .unsafeRuleForType(searchType)
        .sparkType
    } else if (isCollectionType(searchType)) {

      // Extract collection inner type
      val collectionInnerType = unsafelyExtractCollectionType(searchType)
      ArrayType(
        sparkDataTypeOf(
          new SearchField(null, collectionInnerType)
        ),
        containsNull = true
      )
    } else if (isComplexType(searchType)) {

      // Extract subfields,
      // convert them into StructFields
      // and create a StructType
      StructType(JavaScalaConverters
        .listToSeq(searchField.getFields)
        .map(asStructField)
      )
    } else if (isGeoPoint(searchType)) {
      GeoPointRule.sparkType
    } else {
      throw new AzureSparkException(f"Unsupported datatype $searchType")
    }
  }

  /**
   * Convert a search field to a [[StructField]]
   * @param searchField search field
   * @return the equivalent [[StructField]] of this search field
   */

  protected[schema] def asStructField(searchField: SearchField): StructField = {

    StructField(
      searchField.getName,
      sparkDataTypeOf(searchField),
      nullable = true
    )
  }

  /**
   * Convert a SearchIndex schema to a [[StructType]]
   * @param fields search index fields
   * @return the schema of the search index
   */

  def asStructType(fields: Seq[SearchField]): StructType = {

    StructType(
      fields.map(asStructField)
    )
  }
}
