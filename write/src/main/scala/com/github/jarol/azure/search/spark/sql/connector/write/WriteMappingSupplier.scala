package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{FieldAdapter, GeoPointConverter, MappingSupplier}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output.{ArrayConverter, StructTypeConverter, WriteConverter}
import org.apache.spark.sql.types.DataType

/**
 * Mapping supplier for output conversions (i.e. from Spark to Search)
 */

object WriteMappingSupplier
  extends MappingSupplier[WriteConverter] {

  override protected def forAtomicTypes(
                                         spark: DataType,
                                         search: SearchFieldDataType
                                       ): Option[WriteConverter] = {

    None
  }

  override protected def forCollection(sparkType: DataType, search: SearchField, internal: WriteConverter): WriteConverter = ArrayConverter(sparkType, internal)
  override protected def forNested(internal: Map[FieldAdapter, WriteConverter]): WriteConverter = StructTypeConverter(internal)
  override protected def forGeoPoint: WriteConverter = GeoPointConverter.FOR_WRITE
}
