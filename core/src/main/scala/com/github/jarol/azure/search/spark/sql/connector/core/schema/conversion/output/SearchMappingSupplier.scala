package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{AtomicTypeConversionRules, GeoPointRule, SafeMappingSupplier}
import org.apache.spark.sql.types.{DataType, StructField}

/**
 * Supplier of converters to use for extracting values from Spark internal rows
 */

object SearchMappingSupplier
  extends SafeMappingSupplier[StructField, SearchPropertyConverter] {

  override protected def collectionConverter(
                                              sparkType: DataType,
                                              search: SearchField,
                                              internal: SearchPropertyConverter
                                            ): SearchPropertyConverter = {

    ArrayConverter(sparkType, internal)
  }
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
