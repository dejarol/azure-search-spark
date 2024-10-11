package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.DataTypeException
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input.{SearchEncoder, TransformEncoder}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.toSearchTypeOperations

import java.lang.{Double => JDouble, Float => JFloat, Long => JLong}

trait NumericEncoderSupplier {

  protected type TTarget

  protected def fromInt32(v: Integer): TTarget

  protected def fromInt64(v: JLong): TTarget

  protected def fromDouble(v: JDouble): TTarget

  protected def fromSingle(v: JFloat): TTarget

  final def get(searchType: SearchFieldDataType): SearchEncoder = {

    if (searchType.isNumeric) {
      val encodingFunction: Any => TTarget = searchType match {
        case SearchFieldDataType.INT32 => (v1: Any) => fromInt32(v1.asInstanceOf[Integer])
        case SearchFieldDataType.INT64 => (value: Any) => fromInt64(value.asInstanceOf[JLong])
        case SearchFieldDataType.DOUBLE => (value: Any) => fromDouble(value.asInstanceOf[JDouble])
        case SearchFieldDataType.SINGLE => (value: Any) => fromSingle(value.asInstanceOf[JFloat])
        case _ => throw DataTypeException.forUnsupportedSearchType(searchType)
      }

      new TransformEncoder[TTarget] {
        override protected def transform(value: Any): TTarget = encodingFunction(value)
      }
    } else {
      throw new DataTypeException(f"Expected a numeric Search type, found $searchType")
    }
  }
}

object NumericEncoderSupplier {

  case object INT_32
    extends NumericEncoderSupplier {
    override protected type TTarget = Integer
    override protected def fromInt32(v: Integer): Integer = v
    override protected def fromInt64(v: JLong): Integer = v.intValue()
    override protected def fromDouble(v: JDouble): Integer = v.intValue()
    override protected def fromSingle(v: JFloat): Integer = v.intValue()
  }

  case object INT_64 extends NumericEncoderSupplier {
    override protected type TTarget = JLong
    override protected def fromInt32(v: Integer): JLong = v.longValue()
    override protected def fromInt64(v: JLong): JLong = v
    override protected def fromDouble(v: JDouble): JLong = v.longValue()
    override protected def fromSingle(v: JFloat): JLong = v.longValue()
  }

  case object DOUBLE extends NumericEncoderSupplier {
    override protected type TTarget = JDouble
    override protected def fromInt32(v: Integer): JDouble = v.doubleValue()
    override protected def fromInt64(v: JLong): JDouble = v.doubleValue()
    override protected def fromDouble(v: JDouble): JDouble = v
    override protected def fromSingle(v: JFloat): JDouble = v.doubleValue()
  }

  case object SINGLE extends NumericEncoderSupplier {
    override protected type TTarget = JFloat
    override protected def fromInt32(v: Integer): JFloat = v.floatValue()
    override protected def fromInt64(v: JLong): JFloat = v.floatValue()
    override protected def fromDouble(v: JDouble): JFloat = v.floatValue()
    override protected def fromSingle(v: JFloat): JFloat = v
  }
}
