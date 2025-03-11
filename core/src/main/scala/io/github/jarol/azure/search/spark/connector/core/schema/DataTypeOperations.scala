package io.github.jarol.azure.search.spark.connector.core.schema

import io.github.jarol.azure.search.spark.connector.core.DataTypeException

/**
 * Parent class for defining utility methods for both Spark and Search data types
 * @tparam T data type (either a [[org.apache.spark.sql.types.DataType]] or a [[com.azure.search.documents.indexes.models.SearchFieldDataType]])
 * @param description type description (for logging)
 */

abstract class DataTypeOperations[T](protected val input: T,
                                     protected val description: String) {

  /**
   * Returns true if this data type refers to a string
   * @return true for string types
   */

  def isString: Boolean

  /**
   * Returns true if this data type refers to a numeric field
   * @return true for numeric types
   */

  def isNumeric: Boolean

  /**
   * Returns true if this data type refers to a boolean
   * @return true for boolean types
   */

  def isBoolean: Boolean

  /**
   * Returns true if this data type refers to a date time
   * @return true for datetime types
   */

  def isDateTime: Boolean

  /**
   * Return tru if this type is atomic (i.e. string, numeric, boolean or date time)
   * @return true for atomic types
   */

  final def isAtomic: Boolean = isString || isNumeric || isBoolean || isDateTime

  /**
   * Return true if this type is a collection
   * @return true for collection type
   */

  def isCollection: Boolean

  /**
   * Return true if this type represents a complex object (i.e. it defines some subfields)
   * @return true for complex objects
   */

  def isComplex: Boolean

  /**
   * Safely extract the inner collection type (if it's collection)
   * @return a non-empty value if this instance refers to a collection type
   */

  def safeCollectionInnerType: Option[T]

  /**
   * Unsafely extract the collection inner type
   * @throws DataTypeOperations if this instance does not refer to a collection type
   * @return the inner collection type
   */

  @throws[DataTypeException]
  final def unsafeCollectionInnerType: T = {

    safeCollectionInnerType match {
      case Some(value) => value
      case None => throw new DataTypeException(s"Could not retrieve collection inner type for $description $input")
    }
  }
}
