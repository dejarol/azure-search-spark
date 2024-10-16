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
      FieldAdapterImpl(TYPE_LABEL, DataTypes.StringType, 0) -> AtomicEncoders.forUTF8Strings(),
      FieldAdapterImpl(COORDINATES_LABEL, ArrayType(DataTypes.DoubleType), 1) -> CollectionEncoder(AtomicEncoders.identity())
    )
  )

  final val DECODER: SearchDecoder = StructTypeDecoder(
    Map(
      FieldAdapterImpl(TYPE_LABEL, DataTypes.StringType, 0) -> AtomicDecoders.forUTF8Strings(),
      FieldAdapterImpl(COORDINATES_LABEL, ArrayType(DataTypes.DoubleType), 1) -> ArrayDecoder(DataTypes.DoubleType, AtomicDecoders.identity())
    )
  )
}
