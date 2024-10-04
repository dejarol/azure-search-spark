package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input.{AtomicReadConverters, CollectionConverter, ComplexConverter, ReadConverter}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{FieldAdapter, GeoPointRule, SafeMappingSupplierV2}
import org.apache.spark.sql.types.{DataType, DataTypes}

object ReadMappingSupplierV2
  extends SafeMappingSupplierV2[ReadConverter] {

  override protected def forAtomicTypes(
                                         spark: DataType,
                                         search: SearchFieldDataType
                                       ): Option[ReadConverter] = {

    (search, spark) match {
      case (SearchFieldDataType.STRING, DataTypes.StringType) |
           (SearchFieldDataType.DATE_TIME_OFFSET, DataTypes.StringType) => Some(AtomicReadConverters.StringConverter)
      case _ => None
    }
  }

  override protected def forCollection(sparkType: DataType, search: SearchField, internal: ReadConverter): ReadConverter = CollectionConverter(internal)
  override protected def forComplex(internal: Map[FieldAdapter, ReadConverter]): ReadConverter = ComplexConverter(Map.empty)
  override protected def forGeoPoint: ReadConverter = GeoPointRule.readConverter()
}
