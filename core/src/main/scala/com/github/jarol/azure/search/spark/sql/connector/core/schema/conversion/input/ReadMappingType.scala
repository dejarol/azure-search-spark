package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{AtomicTypeConversionRules, GeoPointRule, MappingType}
import org.apache.spark.sql.types.{DataType, StructField}

case object ReadMappingType
  extends MappingType[String, ReadConverter] {

  override protected[conversion] def converterForAtomicTypes(spark: DataType, search: SearchFieldDataType): Option[ReadConverter] = {

    AtomicTypeConversionRules.safeReadConverterForTypes(spark, search)
  }

  override protected[conversion] def collectionConverter(sparkType: DataType, search: SearchField, internal: ReadConverter): ReadConverter = {

    CollectionConverter(internal)
  }

  override protected[conversion] def complexConverter(internal: Map[String, ReadConverter]): ReadConverter = ComplexConverter(internal)
  override protected[conversion] def geoPointConverter: ReadConverter = GeoPointRule.readConverter()
  override protected[conversion] def keyOf(field: StructField): String = field.name
  override protected[conversion] def keyName(key: String): String = key
}
