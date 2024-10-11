package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{SafeCodecSupplier, FieldAdapter, GeoPointType}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output.{ArrayDecoder, AtomicDecoders, SearchDecoder, StructTypeDecoder}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.{toSearchTypeOperations, toSparkTypeOperations}
import org.apache.spark.sql.types.{DataType, DataTypes}

/**
 * Supplier for decoding (i.e. converting data from Spark to Search)
 */

object DecodingSupplier
  extends SafeCodecSupplier[SearchDecoder] {

  override protected def atomicCodecFor(
                                         spark: DataType,
                                         search: SearchFieldDataType
                                       ): Option[SearchDecoder] = {

    if (search.isString) {
      forString(spark)
    } else if (search.isNumeric) {
      forNumeric(spark)
    } else if (search.isBoolean) {
      forBoolean(spark)
    }else if (search.isDateTime) {
      forDateTime(spark)
    } else {
      None
    }
  }

  private def forString(dataType: DataType): Option[SearchDecoder] = {

    if (dataType.isString) {
      Some(AtomicDecoders.STRING)
    } else if (dataType.isNumeric || dataType.isBoolean) {
      Some(AtomicDecoders.STRING_VALUE_OF)
    } else if (dataType.isDateTime) {
      dataType match {
        case DataTypes.DateType => Some(AtomicDecoders.DATE)
        case DataTypes.TimestampType => Some(AtomicDecoders.TIMESTAMP)
        case _ => None
      }
    } else {
      None
    }
  }

  private def forNumeric(dataType: DataType): Option[SearchDecoder] = {

    if (dataType.isNumeric) {

    } else {
      None
    }
  }

  private def forBoolean(dataType: DataType): Option[SearchDecoder] = {

    dataType match {
      case DataTypes.BooleanType => Some(AtomicDecoders.IDENTITY)
      case _ => None
    }
  }

  private def forDateTime(dataType: DataType): Option[SearchDecoder] = {

    dataType match {
      case DataTypes.DateType => Some(AtomicDecoders.DATE)
      case DataTypes.TimestampType => Some(AtomicDecoders.TIMESTAMP)
      case _ => None
    }
  }

  override protected def collectionCodec(sparkType: DataType, search: SearchField, internal: SearchDecoder): SearchDecoder = ArrayDecoder(sparkType, internal)
  override protected def createComplexCodec(internal: Map[FieldAdapter, SearchDecoder]): SearchDecoder = StructTypeDecoder(internal)
  override protected def forGeoPoint: SearchDecoder = GeoPointType.WRITE_CONVERTER
}
