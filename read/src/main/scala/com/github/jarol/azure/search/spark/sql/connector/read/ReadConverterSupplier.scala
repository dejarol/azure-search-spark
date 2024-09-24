package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input.{CollectionConverter, ComplexConverter, ReadConverter}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{AtomicTypeConversionRules, GeoPointRule, SafeConverterSupplier}
import org.apache.spark.sql.types.{DataType, StructField}

/**
 * Supplier of [[ReadConverter]]s (i.e. of objects able to transform Search data objects into Spark data objects)
 */

case object ReadConverterSupplier
  extends SafeConverterSupplier[ReadConverter] {

  override protected def forAtomicTypes(spark: DataType, search: SearchFieldDataType): Option[ReadConverter] = {

    AtomicTypeConversionRules.safeReadConverterForTypes(spark, search)
  }

  override protected def forCollection(sparkType: DataType, search: SearchField, internal: ReadConverter): ReadConverter = {

    CollectionConverter(internal)
  }

  override protected def forComplex(internal: Map[StructField, ReadConverter]): ReadConverter = {

    ComplexConverter(
      internal.map {
        case (k, v) => (k.name, v)
      }
    )
  }

  override protected def forGeoPoint: ReadConverter = GeoPointRule.readConverter()
}
