package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input.{ReadCastConverter, ReadConverter, ReadTransformConverter}

import java.lang

trait NumericCastingSupplier {

  protected def primaryConverter: ReadConverter

  protected def forInt32: Option[ReadTransformConverter[Integer]]

  protected def forInt64: Option[ReadTransformConverter[lang.Long]]

  protected def forDouble: Option[ReadTransformConverter[lang.Double]]

  protected def forSingle: Option[ReadTransformConverter[lang.Float]]

  final def get(searchType: SearchFieldDataType): Option[ReadConverter] = {

    val secondary: Option[ReadConverter] = if (searchType.equals(SearchFieldDataType.INT32)) {
      forInt32
    } else if (searchType.equals(SearchFieldDataType.INT64)) {
      forInt64
    } else if (searchType.equals(SearchFieldDataType.DOUBLE)) {
      forDouble
    } else if (searchType.equals(SearchFieldDataType.SINGLE)) {
      forSingle
    } else None

    secondary.map(primaryConverter.andThen)
      .orElse(Some(primaryConverter))
  }
}

object NumericCastingSupplier {

  case object Integer extends NumericCastingSupplier {
    override protected def primaryConverter: ReadConverter = ReadCastConverter.INT32
    override protected def forInt32: Option[ReadTransformConverter[Integer]] = None
    override protected def forInt64: Option[ReadTransformConverter[lang.Long]] = Some((value: Any) => value.asInstanceOf[lang.Integer].longValue())
    override protected def forDouble: Option[ReadTransformConverter[lang.Double]] = Some((value: Any) => value.asInstanceOf[lang.Integer].doubleValue())
    override protected def forSingle: Option[ReadTransformConverter[lang.Float]] = Some((value: Any) => value.asInstanceOf[lang.Integer].floatValue())
  }

  case object Long extends NumericCastingSupplier {
    override protected def primaryConverter: ReadConverter = ReadCastConverter.INT64
    override protected def forInt32: Option[ReadTransformConverter[Integer]] = Some((value: Any) => value.asInstanceOf[lang.Long].intValue())
    override protected def forInt64: Option[ReadTransformConverter[lang.Long]] = None
    override protected def forDouble: Option[ReadTransformConverter[lang.Double]] = Some((value: Any) => value.asInstanceOf[lang.Integer].doubleValue())
    override protected def forSingle: Option[ReadTransformConverter[lang.Float]] = Some((value: Any) => value.asInstanceOf[lang.Integer].floatValue())
  }

  case object Double extends NumericCastingSupplier {
    override protected def primaryConverter: ReadConverter = ReadCastConverter.DOUBLE
    override protected def forInt32: Option[ReadTransformConverter[Integer]] = Some((value: Any) => value.asInstanceOf[lang.Double].intValue())
    override protected def forInt64: Option[ReadTransformConverter[lang.Long]] = Some((value: Any) => value.asInstanceOf[lang.Double].longValue())
    override protected def forDouble: Option[ReadTransformConverter[lang.Double]] = None
    override protected def forSingle: Option[ReadTransformConverter[lang.Float]] = Some((value: Any) => value.asInstanceOf[lang.Double].floatValue())
  }

  case object Float extends NumericCastingSupplier {
    override protected def primaryConverter: ReadConverter = ReadCastConverter.SINGLE

    override protected def forInt32: Option[ReadTransformConverter[Integer]] = ???

    override protected def forInt64: Option[ReadTransformConverter[lang.Long]] = ???

    override protected def forDouble: Option[ReadTransformConverter[lang.Double]] = ???

    override protected def forSingle: Option[ReadTransformConverter[lang.Float]] = ???
  }
}
