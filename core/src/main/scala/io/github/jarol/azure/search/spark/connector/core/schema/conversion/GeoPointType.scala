package io.github.jarol.azure.search.spark.connector.core.schema.conversion

import io.github.jarol.azure.search.spark.connector.core.schema.conversion.input._
import io.github.jarol.azure.search.spark.connector.core.schema.conversion.output._
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

  /**
   * Create an encoder for this type
   * @param schema schema of candidate GeoPoint field
   * @return an encoder for GeoPoints
   */

  final def encoder(schema: StructType): SearchEncoder = {

    ComplexEncoder(
      Map(
        SearchIndexColumnImpl(
          TYPE_LABEL,
          DataTypes.StringType,
          schema.fieldIndex(TYPE_LABEL)
        ) -> AtomicEncoders.forUTF8Strings(),
        SearchIndexColumnImpl(
          COORDINATES_LABEL,
          ArrayType(DataTypes.DoubleType),
          schema.fieldIndex(COORDINATES_LABEL)
        ) -> CollectionEncoder(AtomicEncoders.identity())
      )
    )
  }

  /**
   * Create a decoder for this type
   * @param schema schema of candidate GeoPoint field
   * @return a decoder for GeoPoints
   */

  final def decoder(schema: StructType): SearchDecoder = {

    StructTypeDecoder(
      Map(
        SearchIndexColumnImpl(
          TYPE_LABEL,
          DataTypes.StringType,
          schema.fieldIndex(TYPE_LABEL)
        ) -> AtomicDecoders.forUTF8Strings(),
        SearchIndexColumnImpl(
          COORDINATES_LABEL,
          ArrayType(DataTypes.DoubleType),
          schema.fieldIndex(COORDINATES_LABEL)
        ) -> ArrayDecoder(DataTypes.DoubleType, AtomicDecoders.identity())
      )
    )
  }
}
