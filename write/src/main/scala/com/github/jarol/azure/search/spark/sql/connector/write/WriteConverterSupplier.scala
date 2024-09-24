package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{AtomicTypeConversionRules, GeoPointRule, SafeConverterSupplier}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output.{ArrayConverter, StructTypeConverter, WriteConverter}
import org.apache.spark.sql.types.{DataType, StructField}

/**
 * Supplier of [[WriteConverter]](s)
 */

case object WriteConverterSupplier
  extends SafeConverterSupplier[WriteConverter] {

  override protected def forAtomicTypes(spark: DataType, search: SearchFieldDataType): Option[WriteConverter] = {

    AtomicTypeConversionRules.safeWriteConverterForTypes(spark, search)
  }

  override protected def forCollection(sparkType: DataType, search: SearchField, internal: WriteConverter): WriteConverter = {

    ArrayConverter(
      sparkType,
      internal
    )
  }

  override protected def forComplex(internal: Map[StructField, WriteConverter]): WriteConverter = StructTypeConverter(internal)
  override protected def forGeoPoint: WriteConverter = GeoPointRule.writeConverter()
}
