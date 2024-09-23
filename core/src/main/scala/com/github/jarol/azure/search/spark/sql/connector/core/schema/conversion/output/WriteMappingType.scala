package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{AtomicTypeConversionRules, GeoPointRule, MappingType}
import org.apache.spark.sql.types.{DataType, StructField}

case object WriteMappingType
  extends MappingType[StructField, WriteConverter] {

  override protected[conversion] def converterForAtomicTypes(spark: DataType, search: SearchFieldDataType): Option[WriteConverter] = {

    AtomicTypeConversionRules.safeWriteConverterForTypes(spark, search)
  }

  override protected[conversion] def collectionConverter(sparkType: DataType, search: SearchField, internal: WriteConverter): WriteConverter = {

    ArrayConverter(
      sparkType,
      internal
    )
  }

  override protected[conversion] def complexConverter(internal: Map[StructField, WriteConverter]): WriteConverter = StructTypeConverter(internal)
  override protected[conversion] def geoPointConverter: WriteConverter = GeoPointRule.writeConverter()
  override protected[conversion] def keyOf(field: StructField): StructField = field
  override protected[conversion] def keyName(key: StructField): String = key.name
}
