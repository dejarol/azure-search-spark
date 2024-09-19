package com.github.jarol.azure.search.spark.sql.connector.schema.conversion.output

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.{AtomicTypeConversionRules, GeoPointRule, SafeMappingSupplier}
import org.apache.spark.sql.types.{DataType, StructField}

/**
 * Supplier of converters to use for extracting values from Spark internal rows
 */

object SearchMappingSupplier
  extends SafeMappingSupplier[StructField, SearchPropertyConverter] {

    override protected def collectionConverter(internal: SearchPropertyConverter): SearchPropertyConverter = ArrayConverter(internal)
    override protected def complexConverter(internal: Map[StructField, SearchPropertyConverter]): SearchPropertyConverter = StructTypeConverter(internal)
    override protected def geoPointConverter: SearchPropertyConverter = GeoPointRule.searchConverter()
    override protected[conversion] def keyFrom(field: StructField): StructField = identity(field)
    override protected[conversion] def nameFrom(key: StructField): String = key.name
    override protected def converterForAtomicTypes(spark: DataType, search: SearchFieldDataType): Option[SearchPropertyConverter] = {

    AtomicTypeConversionRules.safeSearchConverterForTypes(
      spark,
      search
    )
  }
}
