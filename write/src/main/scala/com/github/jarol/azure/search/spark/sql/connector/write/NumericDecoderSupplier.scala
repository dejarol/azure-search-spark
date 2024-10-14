package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output.{SearchDecoder, TransformDecoder}

import java.lang.{Double => JDouble, Float => JFloat, Long => JLong}

trait NumericDecoderSupplier {

  private[write] type SparkType

  private[write] def toInt32(v: SparkType): Integer

  private[write] def toInt64(v: SparkType): JLong

  private[write] def toDouble(v: SparkType): JDouble

  private[write] def toFloat(v: SparkType): JFloat

  final def getForType(searchType: SearchFieldDataType): Option[SearchDecoder] = {

   val decodingFunction: Option[SparkType => Any] = searchType match {
     case SearchFieldDataType.INT32 => Some(toInt32)
     case SearchFieldDataType.INT64 => Some(toInt64)
     case SearchFieldDataType.DOUBLE => Some(toDouble)
     case SearchFieldDataType.SINGLE => Some(toFloat)
     case _ => None
   }

    decodingFunction.map {
      function => new TransformDecoder[Any] {
        override protected def transform(value: Any): Any = function(value.asInstanceOf[SparkType])
      }
    }
  }
}

object NumericDecoderSupplier {

  case object INT32 extends NumericDecoderSupplier {
    override private[write] type SparkType = Integer
    override private[write] def toInt32(v: Integer): Integer = v
    override private[write] def toInt64(v: Integer): JLong = v.longValue()
    override private[write] def toDouble(v: Integer): JDouble = v.doubleValue()
    override private[write] def toFloat(v: Integer): JFloat = v.floatValue()
  }

  case object INT64 extends NumericDecoderSupplier {
    override private[write] type SparkType = JLong
    override private[write] def toInt32(v: JLong): Integer = v.intValue()
    override private[write] def toInt64(v: JLong): JLong = v
    override private[write] def toDouble(v: JLong): JDouble = v.doubleValue()
    override private[write] def toFloat(v: JLong): JFloat = v.floatValue()
  }

  case object DOUBLE extends NumericDecoderSupplier {
    override private[write] type SparkType = JDouble
    override private[write] def toInt32(v: JDouble): Integer = v.intValue()
    override private[write] def toInt64(v: JDouble): JLong = v.longValue()
    override private[write] def toDouble(v: JDouble): JDouble = v
    override private[write] def toFloat(v: JDouble): JFloat = v.floatValue()
  }

  case object SINGLE extends NumericDecoderSupplier {
    override private[write] type SparkType = JFloat
    override private[write] def toInt32(v: JFloat): Integer = v.intValue()
    override private[write] def toInt64(v: JFloat): JLong = v.longValue()
    override private[write] def toDouble(v: JFloat): JDouble = v.doubleValue()
    override private[write] def toFloat(v: JFloat): JFloat = v
  }
}