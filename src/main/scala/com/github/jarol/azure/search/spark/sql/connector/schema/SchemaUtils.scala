package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion._
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * Utilities for inferring the schema of a SearchIndex
 */

object SchemaUtils {

  /**
   * Return the inferred Spark equivalent [[DataType]] for a search field
   * @param searchField a search field
   * @return the equivalent Spark data type for given search field
   */

  def safelyGetInferenceRuleFor(searchField: SearchField): Option[SearchSparkConversionRule] = {

    val searchType = searchField.getType
    if (searchType.isAtomic) {
      Some(AtomicInferSchemaRules.ruleForType(searchType))
    } else if (searchType.isCollection) {
      // Extract collection inner type
      Some(ArrayConversionRule(searchType.unsafelyExtractCollectionType))
    } else if (searchType.isComplex) {
      // Create a complex rule by extracting sub fields
      Some(ComplexConversionRule(searchField.getFields))
    } else if (searchType.isGeoPoint) {
      Some(GeoPointRule)
    } else None
  }

  /**
   * Return the inferred Spark equivalent [[DataType]] for a search field
   * @param searchField a search field
   * @throws AzureSparkException if given search type is not supported
   * @return the equivalent Spark data type for given search field
   */

  @throws[AzureSparkException]
  def inferSparkTypeFor(searchField: SearchField): DataType = {

    safelyGetInferenceRuleFor(searchField) match {
      case Some(value) => value.sparkType()
      case None => throw new AzureSparkException(f"Unsupported datatype ${searchField.getType}")
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
      inferSparkTypeFor(searchField),
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

  def searchFieldCompatibleWith(field: SearchField, f: StructField): Boolean = {

    val sameName = field.getName.equalsIgnoreCase(f.name)
    val compatibleDataType = safelyGetInferenceRuleFor(field).exists {
      _.sparkType().equals(f.dataType)
    } || SchemaConversionRules.existsRuleForTypes(
      f.dataType, field.getType
    )

    sameName && compatibleDataType
  }
}
