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

  final val ENCODER: SearchEncoder = ComplexEncoder(
    Map(
      FieldAdapterImpl(TYPE_LABEL, DataTypes.StringType) -> AtomicEncoders.UTF8_STRING,
      FieldAdapterImpl(COORDINATES_LABEL, ArrayType(DataTypes.DoubleType)) -> CollectionEncoder(AtomicEncoders.IDENTITY)
    )
  )

  final val DECODER: SearchDecoder = StructTypeDecoder(
    Map(
      FieldAdapterImpl(TYPE_LABEL, DataTypes.StringType) -> AtomicDecoders.STRING,
      FieldAdapterImpl(COORDINATES_LABEL, ArrayType(DataTypes.DoubleType)) -> ArrayDecoder(DataTypes.DoubleType, AtomicDecoders.IDENTITY)
    )
  )
}
