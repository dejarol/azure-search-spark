package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{FieldAdapter, GeoPointType, MappingSupplier}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output.{ArrayConverter, StructTypeConverter, WriteConverter}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.toSearchTypeOperations
import org.apache.spark.sql.types.{DataType, DataTypes}

/**
 * Mapping supplier for output conversions (i.e. from Spark to Search)
 */

object WriteMappingSupplier
  extends MappingSupplier[WriteConverter] {

  override protected def forAtomicTypes(
                                         spark: DataType,
                                         search: SearchFieldDataType
                                       ): Option[WriteConverter] = {

    if (search.isDateTime) {
      forDateTime(spark)
    } else {
      None
    }
  }

  private def forDateTime(dataType: DataType): Option[WriteConverter] = {

    dataType match {
      case DataTypes.DateType => None
      case DataTypes.TimestampType => None
      case _ => None
    }
  }

  override protected def forCollection(sparkType: DataType, search: SearchField, internal: WriteConverter): WriteConverter = ArrayConverter(sparkType, internal)
  override protected def forComplex(internal: Map[FieldAdapter, WriteConverter]): WriteConverter = StructTypeConverter(internal)
  override protected def forGeoPoint: WriteConverter = GeoPointType.WRITE_CONVERTER
}
