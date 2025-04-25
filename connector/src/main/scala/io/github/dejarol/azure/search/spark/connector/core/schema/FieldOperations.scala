package io.github.dejarol.azure.search.spark.connector.core.schema

import io.github.dejarol.azure.search.spark.connector.core.DataTypeException

/**
 * Parent trait for managing both Spark fields and Search fields through a unified API.
 * <br>
 * Subclasses should take care of implementing methods related to
 *  - data type evaluation (string, numeric, date, etc ...)
 *  - collection inner type extraction
 *  - subfields extraction
 * @tparam TCollection collection inner type model
 * @tparam TSubField subfield type model
 */

trait FieldOperations[TCollection, TSubField] {

  this: FieldDescriptor =>

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

  def safeCollectionInnerType: Option[TCollection]

  /**
   * Unsafely extract the collection inner type
   * @throws DataTypeOperations if this instance does not refer to a collection type
   * @return the inner collection type
   */

  @throws[DataTypeException]
  final def unsafeCollectionInnerType: TCollection = {

    safeCollectionInnerType match {
      case Some(value) => value
      case None => throw DataTypeException.forNonCollectionField(this)
    }
  }

  /**
   * Safely retrieves the datatype/field's subfields.
   * An empty option is returned for non-complex datatypes/fields
   * @return an optional collection of subfields
   */

  def safeSubFields: Option[Seq[TSubField]]

  /**
   * Retrieves the datatype/field's subfields, or throws an exception if this datatype/field is not complex
   * (i.e. it does not define any subfield).
   * @throws DataTypeException in case of non-complex field
   * @return a collection of fields from this datatype/field
   */

  @throws[DataTypeException]
  final def unsafeSubFields: Seq[TSubField] = {

    safeSubFields match {
      case Some(value) => value
      case None => throw DataTypeException.forNonComplexField(this)
    }
  }
}
