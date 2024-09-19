package com.github.jarol.azure.search.spark.sql.connector.schema.conversion.output

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.SafeMappingSupplier
import org.apache.spark.sql.types.StructField

object SearchMappingSupplier
  extends SafeMappingSupplier[StructField, SearchPropertyConverter] {

  override protected def keyOf(structField: StructField): StructField = identity(structField)
  override protected def nameOf(key: StructField): String = key.name
  override protected def valueOf(structField: StructField, searchField: SearchField): Option[SearchPropertyConverter] = {

    SearchPropertyConverters.safeConverterFor(
      structField, searchField
    )
  }
}
