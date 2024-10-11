package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion

import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input._
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output._
import org.apache.spark.sql.types._

/**
 * GeoPoint type
 */

object GeoPointType {

  private final val TYPE_LABEL = "type"
  private final val COORDINATES_LABEL = "coordinates"
  final val SCHEMA: StructType = StructType(
    Seq(
      StructField(TYPE_LABEL, DataTypes.StringType),
      StructField(COORDINATES_LABEL, ArrayType(DataTypes.DoubleType))
    )
  )

  final val READ_CONVERTER: ReadConverter = ComplexConverter(
    Map(
      FieldAdapterImpl(TYPE_LABEL, DataTypes.StringType) -> ReadConverters.UTF8_STRING,
      FieldAdapterImpl(COORDINATES_LABEL, ArrayType(DataTypes.DoubleType)) -> CollectionConverter(ReadConverters.DOUBLE)
    )
  )

  final val WRITE_CONVERTER: WriteConverter = StructTypeConverter(
    Map(
      FieldAdapterImpl(TYPE_LABEL, DataTypes.StringType) -> AtomicWriteConverters.StringConverter,
      FieldAdapterImpl(COORDINATES_LABEL, ArrayType(DataTypes.DoubleType)) -> ArrayConverter(DataTypes.DoubleType, AtomicWriteConverters.DoubleConverter)
    )
  )
}
