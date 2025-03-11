package io.github.jarol.azure.search.spark.connector.core.schema.conversion

import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * Implementation of [[SearchIndexColumn]]
 * @param fieldName field name
 * @param schemaType Spark type
 * @param fieldIndex field index
 */

case class SearchIndexColumnImpl(
                                  private val fieldName: String,
                                  private val schemaType: DataType,
                                  private val fieldIndex: Int
                                ) extends SearchIndexColumn {

  override def name(): String = fieldName

  override def sparkType(): DataType = schemaType

  override def index(): Int = fieldIndex
}

object SearchIndexColumnImpl {

  /**
   * Create an instance from a [[StructField]]
   * @param field input StructField
   */

  def apply(
             field: StructField,
             schema: StructType
           ): SearchIndexColumnImpl = {

    SearchIndexColumnImpl(
      field.name,
      field.dataType,
      schema.fieldIndex(field.name)
    )
  }
}