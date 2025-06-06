package io.github.dejarol.azure.search.spark.connector.core.schema

import io.github.dejarol.azure.search.spark.connector.core.{DataTypeException, EntityDescription}

/**
 * Parent trait to be implemented by dataTypes or fields in order to access the
 * underlying subfields, in case of a complex dataType or field
 * @tparam T dataType of field type
 */

trait SubFieldsSupplier[T] {

  this: EntityDescription =>

  /**
   * Safely retrieves the datatype/field's subfields.
   * An empty option is returned for non-complex datatypes/fields
   * @return an optional collection of subfields
   */

  def safeSubFields: Option[Seq[T]]

  /**
   * Retrieves the datatype/field's subfields, or throws an exception if this datatype/field is not complex
   * (i.e. it does not define any subfield).
   * @throws DataTypeException in case of non-complex field
   * @return a collection of fields from this datatype/field
   */

  @throws[DataTypeException]
  final def unsafeSubFields: Seq[T] = {

    safeSubFields match {
      case Some(value) => value
      case None => throw DataTypeException.forNonComplexEntity(
        this
      )
    }
  }
}
