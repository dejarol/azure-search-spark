package io.github.dejarol.azure.search.spark.connector.core.schema

import org.apache.spark.sql.types._

/**
 * Object holding Spark equivalent schema of a Search GeoPoint type
 */

object GeoPointType {

  final val TYPE_LABEL = "type"
  final val COORDINATES_LABEL = "coordinates"
  final val SPARK_SCHEMA: StructType = StructType(
    Seq(
      StructField(TYPE_LABEL, DataTypes.StringType),
      StructField(COORDINATES_LABEL, ArrayType(DataTypes.DoubleType))
    )
  )
}