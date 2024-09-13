package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.input.{ArrayConverter, AtomicSparkInternalConverters, ComplexConverter, SparkInternalConverter}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, StructField, StructType}

/**
 * Conversion rule for geo points
 */

case object GeoPointRule
  extends InferSchemaRule {

  final val GEO_POINT_DEFAULT_STRUCT: StructType = StructType(
    Seq(
      StructField("type", DataTypes.StringType),
      StructField("coordinates", ArrayType(DataTypes.DoubleType))
    )
  )

  override def sparkType: DataType = GEO_POINT_DEFAULT_STRUCT
  override def searchType: SearchFieldDataType = SearchFieldDataType.GEOGRAPHY_POINT
  override def converter(): SparkInternalConverter = ComplexConverter(
    Map(
      "type" -> AtomicSparkInternalConverters.StringConverter,
      "coordinates" -> ArrayConverter(AtomicSparkInternalConverters.DoubleConverter)
    )
  )
}
