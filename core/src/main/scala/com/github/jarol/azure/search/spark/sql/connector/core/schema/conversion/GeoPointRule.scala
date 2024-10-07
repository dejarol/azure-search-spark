package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input._
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output._
import org.apache.spark.sql.types._

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
  override def readConverter(): ReadConverter = ComplexConverter(
    Map(
      TYPE_LABEL -> ReadTransformConverter.UTF8_STRING,
      COORDINATES_LABEL -> CollectionConverter(ReadCastConverter.DOUBLE)
    )
  )

  override def writeConverter(): WriteConverter = StructTypeConverter(
    Map(
      StructField(TYPE_LABEL, DataTypes.StringType) -> AtomicWriteConverters.StringConverter,
      StructField(COORDINATES_LABEL, ArrayType(DataTypes.DoubleType)) -> ArrayConverter(DataTypes.DoubleType, AtomicWriteConverters.DoubleConverter)
    )
  )
}
