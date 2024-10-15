package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output.{ArrayDecoder, AtomicDecoders, SearchDecoder, StructTypeDecoder}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{FieldAdapter, GeoPointType, SafeCodecSupplier}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.{toSearchTypeOperations, toSparkTypeOperations}
import org.apache.spark.sql.types.{DataType, DataTypes}

/**
 * Supplier for decoders
 */

object DecodingSupplier
  extends SafeCodecSupplier[SearchDecoder] {

  override protected[write] def atomicCodecFor(
                                                spark: DataType,
                                                search: SearchFieldDataType
                                              ): Option[SearchDecoder] = {

    if (search.isString) {
      forString(spark)
    } else if (search.isNumeric) {
      forNumeric(spark, search)
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
      Some(AtomicDecoders.forStrings())
    } else if (dataType.isNumeric || dataType.isBoolean) {
      Some(AtomicDecoders.stringValueOf())
    } else if (dataType.isDateTime) {
      dataType match {
        case DataTypes.DateType => Some(AtomicDecoders.fromDateToString())
        case DataTypes.TimestampType => Some(AtomicDecoders.forTimestamps())
        case _ => None
      }
    } else {
      None
    }
  }

  private def forNumeric(dataType: DataType, searchType: SearchFieldDataType): Option[SearchDecoder] = {

    if (dataType.isNumeric) {
      val maybeDecoderSupplier: Option[NumericDecoderSupplier] = dataType match {
        case DataTypes.IntegerType => Some(NumericDecoderSupplier.INT32)
        case DataTypes.LongType => Some(NumericDecoderSupplier.INT64)
        case DataTypes.DoubleType => Some(NumericDecoderSupplier.DOUBLE)
        case DataTypes.FloatType => Some(NumericDecoderSupplier.SINGLE)
        case _ => None
      }

      maybeDecoderSupplier.flatMap {
        _.getForType(searchType)
      }
    } else {
      None
    }
  }

  private def forBoolean(dataType: DataType): Option[SearchDecoder] = {

    dataType match {
      case DataTypes.BooleanType => Some(AtomicDecoders.identity())
      case _ => None
    }
  }

  private def forDateTime(dataType: DataType): Option[SearchDecoder] = {

    dataType match {
      case DataTypes.DateType => Some(AtomicDecoders.forDates())
      case DataTypes.TimestampType => Some(AtomicDecoders.forTimestamps())
      case _ => None
    }
  }

  override protected def collectionCodec(sparkType: DataType, internal: SearchDecoder): SearchDecoder = ArrayDecoder(sparkType, internal)
  override protected def createComplexCodec(internal: Map[FieldAdapter, SearchDecoder]): SearchDecoder = StructTypeDecoder(internal)
  override protected def forGeoPoint: SearchDecoder = GeoPointType.DECODER
}
