package com.github.jarol.azure.search.spark.sql.connector.schema.conversion.input

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.SafeMappingSupplier
import org.apache.spark.sql.types.StructField

object SparkInternalMappingSupplier
  extends SafeMappingSupplier[String, SparkInternalConverter] {

  override protected def keyOf(structField: StructField): String = structField.name
  override protected def nameOf(key: String): String = identity(key)
  override protected def valueOf(structField: StructField, searchField: SearchField): Option[SparkInternalConverter] = {

    SparkInternalConverters.safeConverterFor(
      structField, searchField
    )
  }
}

