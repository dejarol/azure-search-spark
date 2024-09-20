package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{AtomicTypeConversionRules, GeoPointRule, SafeMappingSupplier}
import org.apache.spark.sql.types.{DataType, StructField}

/**
 * Supplier of converters for extracting values from Search documents
 */

object SparkMappingSupplier
  extends SafeMappingSupplier[String, SparkInternalConverter] {

  override protected def collectionConverter(internal: SparkInternalConverter): SparkInternalConverter = CollectionConverter(internal)
  override protected def complexConverter(internal: Map[String, SparkInternalConverter]): SparkInternalConverter = ComplexConverter(internal)
  override protected def geoPointConverter: SparkInternalConverter = GeoPointRule.sparkConverter()
  override protected[conversion] def keyFrom(field: StructField): String = field.name
  override protected[conversion] def nameFrom(key: String): String = identity(key)

  override protected def converterForAtomicTypes(spark: DataType, search: SearchFieldDataType): Option[SparkInternalConverter] = {

    AtomicTypeConversionRules.safeSparkConverterForTypes(
      spark,
      search
    )
  }
}

