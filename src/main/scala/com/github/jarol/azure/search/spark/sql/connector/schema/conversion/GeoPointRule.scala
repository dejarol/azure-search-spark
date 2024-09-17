package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.input.{ArrayConverter, AtomicSparkInternalConverters, ComplexConverter, SparkInternalConverter}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, StructField, StructType}

/**
 * Conversion rule for geo points
 */

case object GeoPointRule
  extends InferSchemaRule {

  private final val TYPE_LABEL = "type"
  private final val COORDINATES_LABEL = "coordinates"

  /**
   * Default schema for Geopoints in Spark
   */

  final val GEO_POINT_DEFAULT_STRUCT: StructType = StructType(
    Seq(
      StructField(TYPE_LABEL, DataTypes.StringType),
      StructField(COORDINATES_LABEL, ArrayType(DataTypes.DoubleType))
    )
  )

  override def sparkType: DataType = GEO_POINT_DEFAULT_STRUCT
  override def searchType: SearchFieldDataType = SearchFieldDataType.GEOGRAPHY_POINT
  override def converter(): SparkInternalConverter = ComplexConverter(
    Map(
      TYPE_LABEL -> AtomicSparkInternalConverters.StringConverter,
      COORDINATES_LABEL -> ArrayConverter(AtomicSparkInternalConverters.DoubleConverter)
    )
  )
}
