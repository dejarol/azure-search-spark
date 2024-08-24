package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.types.conversion.{AtomicInferSchemaRules, GeoPointRule}
import com.github.jarol.azure.search.spark.sql.connector.types.implicits._
import com.github.jarol.azure.search.spark.sql.connector.{AzureSparkException, JavaScalaConverters}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

/**
 * Utilities for inferring the schema of a SearchIndex
 */

object SchemaUtils {

  /**
   * Return the Spark equivalent [[DataType]] for a search field
   * @param searchField a search field
   * @throws AzureSparkException if given search type is not supported
   * @return the equivalent Spark data type for given search field
   */

  @throws[AzureSparkException]
  def sparkDataTypeOf(searchField: SearchField): DataType = {

    val searchType = searchField.getType
    if (searchType.isAtomic) {
      AtomicInferSchemaRules
        .ruleForType(searchType)
        .sparkType
    } else if (searchType.isCollection) {

      // Extract collection inner type
      ArrayType(
        sparkDataTypeOf(
          new SearchField(null, searchType.unsafelyExtractCollectionType)
        ),
        containsNull = true
      )
    } else if (searchType.isComplex) {

      // Extract subfields, convert them into StructFields and create a StructType
      StructType(
        JavaScalaConverters
          .listToSeq(searchField.getFields)
          .map(asStructField)
      )
    } else if (searchType.isGeoPoint) {
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
