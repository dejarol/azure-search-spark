package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion

import org.apache.spark.sql.types.{DataType, StructField}

case class FieldAdapter(
                         name: String,
                         schemaType: DataType
                       ) {

  def this(field: StructField) = {

    this(
      field.name,
      field.dataType
    )
  }
}