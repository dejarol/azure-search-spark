package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output.{ArrayDecoder, AtomicDecoders, SearchDecoder, StructTypeDecoder}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{GeoPointType, SafeCodecSupplier, SearchIndexColumn}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.{toSearchTypeOperations, toSparkTypeOperations}
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}

/**
 * Supplier for decoders
 */

object DecodersSupplier
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

  /**
   * Returns the decoder to use for decoding Spark internal values to Search String type
   * @param dataType Spark internal type
   * @return the decoder for strings
   */

  private def forString(dataType: DataType): Option[SearchDecoder] = {

    if (dataType.isString) {
      // String to String
      Some(AtomicDecoders.forUTF8Strings())
    } else if (dataType.isNumeric || dataType.isBoolean) {
      // Numeric or boolean to String
      Some(AtomicDecoders.stringValueOf())
    } else if (dataType.isDateTime) {
      // Date or Timestamp to String
      dataType match {
        case DataTypes.DateType => Some(AtomicDecoders.fromDateToString())
        case DataTypes.TimestampType => Some(AtomicDecoders.forTimestamps())
        case _ => None
      }
    } else {
      None
    }
  }

  /**
   * Returns the decoder to user for decoding Spark internal numeric values to Search numeric types
   * @param dataType Spark internal type
   * @param searchType Search numeric type
   * @return decoder for numeric types
   */

  private def forNumeric(dataType: DataType, searchType: SearchFieldDataType): Option[SearchDecoder] = {

    // A decoder will exist only for Spark internal numeric types
    if (dataType.isNumeric) {
      Some(
        NumericDecoder(dataType, searchType)
      )
    } else {
      None
    }
  }

  /**
   * Returns the decoder to user for decoding Spark internal values to Search boolean type
   * @param dataType Spark internal type
   * @return decoder for boolean types
   */

  private def forBoolean(dataType: DataType): Option[SearchDecoder] = {

    dataType match {
      // Boolean to Boolean
      case DataTypes.BooleanType => Some(AtomicDecoders.identity())
      case _ => None
    }
  }

  /**
   * Returns the decoder to user for decoding Spark internal values to Search datetime type
   * @param dataType Spark internal type
   * @return decoder for datetime types
   */

  private def forDateTime(dataType: DataType): Option[SearchDecoder] = {

    // A decoder will exist only for Date or Timestamp Spark types
    dataType match {
      case DataTypes.DateType => Some(AtomicDecoders.forDates())
      case DataTypes.TimestampType => Some(AtomicDecoders.forTimestamps())
      case _ => None
    }
  }

  override protected def collectionCodec(sparkType: DataType, internal: SearchDecoder): SearchDecoder = ArrayDecoder(sparkType, internal)
  override protected def createComplexCodec(internal: Map[SearchIndexColumn, SearchDecoder]): SearchDecoder = StructTypeDecoder(internal)
  override protected def forGeoPoint(schema: StructType): SearchDecoder = GeoPointType.decoder(schema)
}
