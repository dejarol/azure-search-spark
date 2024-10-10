package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion

import org.apache.spark.sql.types.{DataType, StructField}

/**
 * Implementation of [[FieldAdapter]]
 * @param fieldName field name
 * @param schemaType Spark type
 */

case class FieldAdapterImpl(
                             private val fieldName: String,
                             private val schemaType: DataType
                           ) extends FieldAdapter {

  override def name(): String = fieldName

  override def sparkType(): DataType = schemaType
}

object FieldAdapterImpl {

  /**
   * Create an instance from a [[StructField]]
   * @param field input StructField
   */

  def apply(field: StructField): FieldAdapterImpl = {

    FieldAdapterImpl(
      field.name,
      field.dataType
    )
  }
}