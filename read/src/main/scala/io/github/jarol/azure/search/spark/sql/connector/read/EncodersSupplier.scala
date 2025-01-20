package io.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchFieldDataType
import io.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input.{AtomicEncoders, CollectionEncoder, ComplexEncoder, SearchEncoder}
import io.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{GeoPointType, SafeCodecSupplier, SearchIndexColumn}
import io.github.jarol.azure.search.spark.sql.connector.core.schema.{toSearchTypeOperations, toSparkTypeOperations}
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}

/**
 * Encoder supplier
 */

object EncodersSupplier
  extends SafeCodecSupplier[SearchEncoder] {

  /**
   * Safely get the encoder between two atomic types
   * <br>
   * The encoder will exist only for compatible atomic types
   * @param spark  Spark type
   * @param search Search type
   * @return an optional codec for given types
   */

  override protected[read] def atomicCodecFor(
                                               spark: DataType,
                                               search: SearchFieldDataType
                                             ): Option[SearchEncoder] = {

    if (search.isString) {
      forString(spark)
    } else if (search.isNumeric) {
      forNumericTypes(spark, search)
    } else if (search.isBoolean) {
      forBoolean(spark)
    } else if (search.isDateTime) {
      forDateTime(spark)
    } else {
      None
    }
  }

  /**
   * Get the encoder to use when dealing with Search STRING type
   * @param dataType Spark target type
   * @return the encoder for Search STRING type
   */

  private def forString(dataType: DataType): Option[SearchEncoder] = {

    // Allow a string to be read only as a string
    dataType match {
      case DataTypes.StringType => Some(AtomicEncoders.forUTF8Strings())
      case _ => None
    }
  }

  /**
   * Get the encoder to use for Search numeric types (int32, int64, double, single)
   * @param dataType Spark data type
   * @param searchType Search numeric type
   * @return encoder for Search numeric types
   */

  private def forNumericTypes(
                               dataType: DataType,
                               searchType: SearchFieldDataType
                             ): Option[SearchEncoder] = {

    // Allow a numeric type to be read as
    // [a] another numeric type
    // [b] a string
    if (dataType.isNumeric) {
      Some(
        NumericEncoder(
          searchType,
          dataType
        )
      )
    } else {

      // Set encoder for strings
      dataType match {
        case DataTypes.StringType => Some(
          AtomicEncoders.stringValueOf().andThen(
            AtomicEncoders.forUTF8Strings()
          )
        )
        case _ => None
      }
    }
  }

  /**
   * Get the encoder for Search BOOLEAN type
   * @param dataType Spark data type
   * @return encoder for Search BOOLEAN type
   */

  private def forBoolean(dataType: DataType): Option[SearchEncoder] = {

    // Allow a boolean to be read as
    // [a] a boolean
    // [b] a string
    dataType match {
      case DataTypes.StringType => Some(AtomicEncoders.stringValueOf().andThen(AtomicEncoders.forUTF8Strings()))
      case DataTypes.BooleanType => Some(AtomicEncoders.identity())
      case _ => None
    }
  }

  /**
   * Get the encoder for Search DATETIME_OFFSET type
   * @param dataType Spark data type
   * @return encoder for Search DATETIME_OFFSET type
   */

  private def forDateTime(dataType: DataType): Option[SearchEncoder] = {

    // Allow a datetime to be read as
    // [a] a date or timestamp
    // [b] a string
    dataType match {
      case DataTypes.TimestampType => Some(AtomicEncoders.forTimestamps())
      case DataTypes.DateType => Some(AtomicEncoders.forDates())
      case DataTypes.StringType => Some(AtomicEncoders.forUTF8Strings())
      case _ => None
    }
  }

  override protected def collectionCodec(sparkType: DataType, internal: SearchEncoder): SearchEncoder = CollectionEncoder(internal)
  override protected def createComplexCodec(internal: Map[SearchIndexColumn, SearchEncoder]): SearchEncoder = ComplexEncoder(internal)
  override protected def forGeoPoint(schema: StructType): SearchEncoder = GeoPointType.encoder(schema)
}
