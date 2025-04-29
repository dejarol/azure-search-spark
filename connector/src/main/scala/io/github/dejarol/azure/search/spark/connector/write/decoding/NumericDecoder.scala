package io.github.dejarol.azure.search.spark.connector.write.decoding

import com.azure.search.documents.indexes.models.SearchFieldDataType
import org.apache.spark.sql.types.{DataType, DataTypes}

import java.lang.{Double => JDouble, Float => JFloat, Long => JLong}

/**
 * Decoder for numeric types
 * @param dataType Spark type
 * @param searchType Search type
 */

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

  /**
   * Decode an integer
   * @param v integer value
   * @return the decoded value
   */

  private def fromInt(v: Integer): AnyRef = {

    searchType match {
      case SearchFieldDataType.INT32 => v
      case SearchFieldDataType.INT64 => JLong.valueOf(v.longValue())
      case SearchFieldDataType.DOUBLE => JDouble.valueOf(v.doubleValue())
    }
  }

  /**
   * Decode a long
   * @param l long value
   * @return the decoded value
   */

  private def fromLong(l: JLong): AnyRef = {

    searchType match {
      case SearchFieldDataType.INT32 => Integer.valueOf(l.intValue())
      case SearchFieldDataType.INT64 => l
      case SearchFieldDataType.DOUBLE => JDouble.valueOf(l.doubleValue())
    }
  }

  /**
   * Decode a double
   * @param d double value
   * @return the decoded value
   */

  private def fromDouble(d: JDouble): AnyRef = {

    searchType match {
      case SearchFieldDataType.INT32 => Integer.valueOf(d.intValue())
      case SearchFieldDataType.INT64 => JLong.valueOf(d.longValue())
      case SearchFieldDataType.DOUBLE => d
    }
  }

  /**
   * Decode a float
   * @param f float value
   * @return the decoded value
   */

  private def fromFloat(f: JFloat): AnyRef = {

    searchType match {
      case SearchFieldDataType.INT32 => Integer.valueOf(f.intValue())
      case SearchFieldDataType.INT64 => JLong.valueOf(f.longValue())
      case SearchFieldDataType.DOUBLE => JDouble.valueOf(f.doubleValue())
    }
  }
}
