package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{FieldAdapter, GeoPointRule, SafeMappingSupplierV2}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input.{AtomicReadConverters, CollectionConverter, ComplexConverter, ReadConverter}
import org.apache.spark.sql.types.{DataType, DataTypes}

object ReadMappingSupplierV2
  extends SafeMappingSupplierV2[ReadConverter] {

  override protected def forAtomicTypes(
                                         spark: DataType,
                                         search: SearchFieldDataType
                                       ): Either[String, ReadConverter] = {

    (spark, search) match {
      case (DataTypes.StringType, SearchFieldDataType.STRING) => Right(AtomicReadConverters.StringConverter)
      case _ => Left("no read converter found")
    }
  }

  override protected def forCollection(sparkType: DataType, search: SearchField, internal: ReadConverter): ReadConverter = CollectionConverter(internal)
  override protected def forComplex(internal: Map[FieldAdapter, ReadConverter]): ReadConverter = ComplexConverter(Map.empty)
  override protected def forGeoPoint: ReadConverter = GeoPointRule.readConverter()
}
