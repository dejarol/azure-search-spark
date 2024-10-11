package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.schema._
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input._
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{FieldAdapter, GeoPointType, CodecSupplier}
import org.apache.spark.sql.types.{DataType, DataTypes}

object EncodingSupplier
  extends CodecSupplier[SearchEncoder] {

  override protected[read] def forAtomicTypes(
                                               spark: DataType,
                                               search: SearchFieldDataType
                                             ): Option[SearchEncoder] = {

    if (search.isString) {
      forString(spark)
    } else if (search.isNumeric) {
      forNumericTypes(search, spark)
    } else if (search.isBoolean) {
      forBoolean(spark)
    } else if (search.isDateTime) {
      forDateTime(spark)
    } else {
      None
    }
  }

  private def forString(dataType: DataType): Option[SearchEncoder] = {

    dataType match {
      case DataTypes.StringType => Some(AtomicEncoders.UTF8_STRING)
      case _ => None
    }
  }

  private def forNumericTypes(
                               searchType: SearchFieldDataType,
                               dataType: DataType
                             ): Option[SearchEncoder] = {

    if (dataType.isNumeric) {

      val numericEncoderSupplier: Option[NumericEncoderSupplier] = dataType match {
        case DataTypes.IntegerType => Some(NumericEncoderSupplier.INT_32)
        case DataTypes.LongType => Some(NumericEncoderSupplier.INT_64)
        case DataTypes.DoubleType => Some(NumericEncoderSupplier.DOUBLE)
        case DataTypes.FloatType => Some(NumericEncoderSupplier.SINGLE)
        case _ => None
      }

      numericEncoderSupplier.map {
        _.get(searchType)
      }
    } else {
      dataType match {
        case DataTypes.StringType => Some(
          AtomicEncoders.STRING_VALUE_OF.andThen(
            AtomicEncoders.UTF8_STRING)
        )
        case _ => None
      }
    }
  }

  private def forBoolean(dataType: DataType): Option[SearchEncoder] = {

    dataType match {
      case DataTypes.StringType => Some(AtomicEncoders.STRING_VALUE_OF.andThen(AtomicEncoders.UTF8_STRING))
      case DataTypes.BooleanType => Some(AtomicEncoders.IDENTITY)
      case _ => None
    }
  }

  private def forDateTime(dataType: DataType): Option[SearchEncoder] = {

    dataType match {
      case DataTypes.TimestampType => Some(AtomicEncoders.TIMESTAMP)
      case DataTypes.DateType => Some(AtomicEncoders.DATE)
      case DataTypes.StringType => Some(AtomicEncoders.UTF8_STRING)
      case _ => None
    }
  }

  override protected def forCollection(sparkType: DataType, search: SearchField, internal: SearchEncoder): SearchEncoder = CollectionEncoder(internal)
  override protected def forComplex(internal: Map[FieldAdapter, SearchEncoder]): SearchEncoder = ComplexEncoder(internal)
  override protected def forGeoPoint: SearchEncoder = GeoPointType.READ_CONVERTER
}
