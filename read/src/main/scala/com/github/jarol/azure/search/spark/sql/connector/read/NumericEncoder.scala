package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input.TransformEncoder
import org.apache.spark.sql.types.{DataType, DataTypes}

import java.lang.{Double => JDouble, Long => JLong}

/**
 * Encoder for numeric types
 * <br>
 * @param searchType Search data type (input)
 * @param dataType Spark data type (target)
 */

case class NumericEncoder(
                           private val searchType: SearchFieldDataType,
                           private val dataType: DataType,
                         ) extends TransformEncoder[Object] {

  override protected def transform(value: Any): AnyRef  = {

    (value, searchType) match {
      case (v: Integer, SearchFieldDataType.INT32) => fromInt(v)
      case (v: Integer, SearchFieldDataType.INT64) => fromInt(v)
      case (v: JLong, SearchFieldDataType.INT64) => fromLong(v)
      case (v: JDouble, SearchFieldDataType.DOUBLE) => fromDouble(v)
    }
  }

  /**
   * Encode the Search value, when it's an integer
   * @param v integer value
   * @return the encoded value
   */

  private def fromInt(v: Integer): AnyRef = {

    dataType match {
      case DataTypes.IntegerType => v
      case DataTypes.LongType => JLong.valueOf(v.longValue())
      case DataTypes.DoubleType => JDouble.valueOf(v.doubleValue())
    }
  }

  /**
   * Encode the Search value, when it's a long
   * @param v long value
   * @return the encoded value
   */

  private def fromLong(v: JLong): AnyRef = {

    dataType match {
      case DataTypes.IntegerType => Integer.valueOf(v.intValue())
      case DataTypes.LongType => v
      case DataTypes.DoubleType => JDouble.valueOf(v.doubleValue())
    }
  }

  /**
   * Encode the Search value, when it's a double
   * @param v integer value
   * @return the encoded value
   */

  private def fromDouble(v: JDouble): AnyRef = {

    dataType match {
      case DataTypes.IntegerType => Integer.valueOf(v.intValue())
      case DataTypes.LongType => JLong.valueOf(v.longValue())
      case DataTypes.DoubleType => v
    }
  }
}