package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input.{ReadCastConverter, ReadConverter, ReadConverters, ReadTransformConverter}

import java.lang

trait NumericCastingSupplier {

  protected type InnerType

  protected def defaultConverter: ReadCastConverter[InnerType]

  protected def fromInt32(v: Integer): InnerType

  protected def fromInt64(v: lang.Long): InnerType

  protected def fromDouble(v: lang.Double): InnerType

  protected def fromSingle(v: lang.Float): InnerType

  final def get(searchType: SearchFieldDataType): Option[ReadConverter] = {

    val secondary: Option[ReadTransformConverter[InnerType]] = if (searchType.equals(SearchFieldDataType.INT32)) {
      Some((value: Any) => fromInt32(value.asInstanceOf[Integer]))
    } else if (searchType.equals(SearchFieldDataType.INT64)) {
      Some((value: Any) => fromInt64(value.asInstanceOf[lang.Long]))
    } else if (searchType.equals(SearchFieldDataType.DOUBLE)) {
      Some((value: Any) => fromDouble(value.asInstanceOf[lang.Double]))
    } else if (searchType.equals(SearchFieldDataType.SINGLE)) {
      Some((value: Any) => fromSingle(value.asInstanceOf[lang.Float]))
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
    override protected def fromInt64(v: lang.Long): Integer = v.intValue()
    override protected def fromDouble(v: lang.Double): Integer = v.intValue()
    override protected def fromSingle(v: lang.Float): Integer = v.intValue()
  }

  case object INT_64 extends NumericCastingSupplier {
    override protected type InnerType = lang.Long
    override protected def defaultConverter: ReadCastConverter[lang.Long] = ReadConverters.INT64
    override protected def fromInt32(v: Integer): lang.Long = v.longValue()
    override protected def fromInt64(v: lang.Long): lang.Long = v
    override protected def fromDouble(v: lang.Double): lang.Long = v.longValue()
    override protected def fromSingle(v: lang.Float): lang.Long = v.longValue()
  }

  case object DOUBLE extends NumericCastingSupplier {
    override protected type InnerType = lang.Double
    override protected def defaultConverter: ReadCastConverter[lang.Double] = ReadConverters.DOUBLE
    override protected def fromInt32(v: Integer): lang.Double = v.doubleValue()
    override protected def fromInt64(v: lang.Long): lang.Double = v.doubleValue()
    override protected def fromDouble(v: lang.Double): lang.Double = v
    override protected def fromSingle(v: lang.Float): lang.Double = v.doubleValue()
  }

  case object SINGLE extends NumericCastingSupplier {
    override protected type InnerType = lang.Float
    override protected def defaultConverter: ReadCastConverter[lang.Float] = ReadConverters.SINGLE
    override protected def fromInt32(v: Integer): lang.Float = v.floatValue()
    override protected def fromInt64(v: lang.Long): lang.Float = v.floatValue()
    override protected def fromDouble(v: lang.Double): lang.Float = v.floatValue()
    override protected def fromSingle(v: lang.Float): lang.Float = v
  }
}
