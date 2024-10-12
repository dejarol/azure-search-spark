package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input.{SearchEncoder, TransformEncoder}

import java.lang.{Double => JDouble, Float => JFloat, Long => JLong}

/**
 * Supplier of encoders for handling conversion between numeric types.
 * <br>
 * Each implementation should define a target numeric type, and should define proper conversion functions
 * for each supported numeric types to the instance's target type
 */

trait NumericEncoderSupplier {

  /**
   * Target type
   */

  private[read] type TargetType

  /**
   * Conversion from an integer to this instance's target type
   * @param v integer value
   * @return an instance of target's type
   */

  private[read] def fromInt32(v: Integer): TargetType

  /**
   * Conversion from a Long to this instance's target type
   * @param v long value
   * @return an instance of target's type
   */

  private[read] def fromInt64(v: JLong): TargetType

  /**
   * Conversion from a double to this instance's target type
   * @param v double value
   * @return an instance of target's type
   */

  private[read] def fromDouble(v: JDouble): TargetType

  /**
   * Conversion from a float to this instance's target type
   * @param v float value
   * @return an instance of target's type
   */

  private[read] def fromSingle(v: JFloat): TargetType

  /**
   * Safely get an encoder that will convert data from a given Search type to this instance's target type
   * @param searchType search type
   * @return an optional [[SearchEncoder]]
   */

  final def getForType(searchType: SearchFieldDataType): Option[SearchEncoder] = {

    // Set the encoding function depending on given Search type
    // If, for instance, the input Search type is a INT32, an Integer is expected.
    // SO, we have to first convert the value to an Integer and then map this value to the target type
    val encodingFunction: Option[Any => TargetType] = searchType match {
      case SearchFieldDataType.INT32 => Some((v1: Any) => fromInt32(v1.asInstanceOf[Integer]))
      case SearchFieldDataType.INT64 => Some((v1: Any) => fromInt64(v1.asInstanceOf[JLong]))
      case SearchFieldDataType.DOUBLE => Some((v1: Any) => fromDouble(v1.asInstanceOf[JDouble]))
      case SearchFieldDataType.SINGLE => Some((v1: Any) => fromSingle(v1.asInstanceOf[JFloat]))
      case _ => None
    }

    // Use the encoding function to create a TransformEncoder
    encodingFunction.map {
      function => new TransformEncoder[TargetType] {
        override protected def transform(value: Any): TargetType =
          function(value)
      }
    }
  }
}

object NumericEncoderSupplier {

  /**
   * Numeric supplier for Spark Integer type
   */

  case object INT_32
    extends NumericEncoderSupplier {
    override private[read] type TargetType = Integer
    override private[read] def fromInt32(v: Integer): Integer = v
    override private[read] def fromInt64(v: JLong): Integer = v.intValue()
    override private[read] def fromDouble(v: JDouble): Integer = v.intValue()
    override private[read] def fromSingle(v: JFloat): Integer = v.intValue()
  }

  /**
   * Numeric supplier for Spark Long type
   */

  case object INT_64 extends NumericEncoderSupplier {
    override private[read] type TargetType = JLong
    override private[read] def fromInt32(v: Integer): JLong = v.longValue()
    override private[read] def fromInt64(v: JLong): JLong = v
    override private[read] def fromDouble(v: JDouble): JLong = v.longValue()
    override private[read] def fromSingle(v: JFloat): JLong = v.longValue()
  }

  /**
   * Numeric supplier for Spark double type
   */

  case object DOUBLE extends NumericEncoderSupplier {
    override private[read] type TargetType = JDouble
    override private[read] def fromInt32(v: Integer): JDouble = v.doubleValue()
    override private[read] def fromInt64(v: JLong): JDouble = v.doubleValue()
    override private[read] def fromDouble(v: JDouble): JDouble = v
    override private[read] def fromSingle(v: JFloat): JDouble = v.doubleValue()
  }

  /**
   * Numeric supplier for Spark float type
   */

  case object SINGLE extends NumericEncoderSupplier {
    override private[read] type TargetType = JFloat
    override private[read] def fromInt32(v: Integer): JFloat = v.floatValue()
    override private[read] def fromInt64(v: JLong): JFloat = v.floatValue()
    override private[read] def fromDouble(v: JDouble): JFloat = v.floatValue()
    override private[read] def fromSingle(v: JFloat): JFloat = v
  }
}
