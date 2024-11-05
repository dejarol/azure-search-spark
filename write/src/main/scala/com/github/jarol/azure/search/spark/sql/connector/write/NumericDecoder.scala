package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output.TransformDecoder
import org.apache.spark.sql.types.{DataType, DataTypes}

import java.lang.{Double => JDouble, Float => JFloat, Long => JLong}

case class NumericDecoder(
                           private val dataType: DataType,
                           private val searchType: SearchFieldDataType
                         ) extends TransformDecoder[AnyRef] {

  override protected def transform(value: Any): AnyRef = {

    (value, dataType) match {
      case (i: Integer, DataTypes.IntegerType) => fromInt(i)
      case (l: JLong, DataTypes.LongType) => fromLong(l)
      case (d: JDouble, DataTypes.DoubleType) => fromDouble(d)
      case (f: JFloat, DataTypes.FloatType) => fromFloat(f)
    }
  }

  private def fromInt(v: Integer): AnyRef = {

    searchType match {
      case SearchFieldDataType.INT32 => v
      case SearchFieldDataType.INT64 => JLong.valueOf(v.longValue())
      case SearchFieldDataType.DOUBLE => JDouble.valueOf(v.doubleValue())
    }
  }

  private def fromLong(v: JLong): AnyRef = {

    searchType match {
      case SearchFieldDataType.INT32 => Integer.valueOf(v.intValue())
      case SearchFieldDataType.INT64 => v
      case SearchFieldDataType.DOUBLE => JDouble.valueOf(v.doubleValue())
    }
  }

  private def fromDouble(d: JDouble): AnyRef = {

    searchType match {
      case SearchFieldDataType.INT32 => Integer.valueOf(d.intValue())
      case SearchFieldDataType.INT64 => JLong.valueOf(d.longValue())
      case SearchFieldDataType.DOUBLE => d
    }
  }

  private def fromFloat(f: JFloat): AnyRef = {

    searchType match {
      case SearchFieldDataType.INT32 => Integer.valueOf(f.intValue())
      case SearchFieldDataType.INT64 => JLong.valueOf(f.longValue())
      case SearchFieldDataType.DOUBLE => JDouble.valueOf(f.doubleValue())
    }
  }
}
