package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input.{ReadCastConverter, ReadConverter, ReadConverters, ReadTransformConverter}

import java.lang.{Double => JDouble, Float => JFloat, Long => JLong}

trait NumericCastingSupplier {

  protected type InnerType

  protected def defaultConverter: ReadCastConverter[InnerType]

  protected def fromInt32(v: Integer): InnerType

  protected def fromInt64(v: JLong): InnerType

  protected def fromDouble(v: JDouble): InnerType

  protected def fromSingle(v: JFloat): InnerType

  final def get(searchType: SearchFieldDataType): Option[ReadConverter] = {

    val secondary: Option[ReadTransformConverter[InnerType]] = if (searchType.equals(SearchFieldDataType.INT32)) {
      Some((value: Any) => fromInt32(value.asInstanceOf[Integer]))
    } else if (searchType.equals(SearchFieldDataType.INT64)) {
      Some((value: Any) => fromInt64(value.asInstanceOf[JLong]))
    } else if (searchType.equals(SearchFieldDataType.DOUBLE)) {
      Some((value: Any) => fromDouble(value.asInstanceOf[JDouble]))
    } else if (searchType.equals(SearchFieldDataType.SINGLE)) {
      Some((value: Any) => fromSingle(value.asInstanceOf[JFloat]))
    } else None

    secondary.map(defaultConverter.andThen)
      .orElse(Some(defaultConverter))
  }
}

object NumericCastingSupplier {

  case object INT_32
    extends NumericCastingSupplier {
    override protected type InnerType = Integer
    override protected def defaultConverter: ReadCastConverter[Integer] = ReadConverters.INT32
    override protected def fromInt32(v: Integer): Integer = v
    override protected def fromInt64(v: JLong): Integer = v.intValue()
    override protected def fromDouble(v: JDouble): Integer = v.intValue()
    override protected def fromSingle(v: JFloat): Integer = v.intValue()
  }

  case object INT_64 extends NumericCastingSupplier {
    override protected type InnerType = JLong
    override protected def defaultConverter: ReadCastConverter[JLong] = ReadConverters.INT64
    override protected def fromInt32(v: Integer): JLong = v.longValue()
    override protected def fromInt64(v: JLong): JLong = v
    override protected def fromDouble(v: JDouble): JLong = v.longValue()
    override protected def fromSingle(v: JFloat): JLong = v.longValue()
  }

  case object DOUBLE extends NumericCastingSupplier {
    override protected type InnerType = JDouble
    override protected def defaultConverter: ReadCastConverter[JDouble] = ReadConverters.DOUBLE
    override protected def fromInt32(v: Integer): JDouble = v.doubleValue()
    override protected def fromInt64(v: JLong): JDouble = v.doubleValue()
    override protected def fromDouble(v: JDouble): JDouble = v
    override protected def fromSingle(v: JFloat): JDouble = v.doubleValue()
  }

  case object SINGLE extends NumericCastingSupplier {
    override protected type InnerType = JFloat
    override protected def defaultConverter: ReadCastConverter[JFloat] = ReadConverters.SINGLE
    override protected def fromInt32(v: Integer): JFloat = v.floatValue()
    override protected def fromInt64(v: JLong): JFloat = v.floatValue()
    override protected def fromDouble(v: JDouble): JFloat = v.floatValue()
    override protected def fromSingle(v: JFloat): JFloat = v
  }
}
