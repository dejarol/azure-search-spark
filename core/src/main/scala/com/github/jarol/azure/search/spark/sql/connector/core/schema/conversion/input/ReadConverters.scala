package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input

import com.github.jarol.azure.search.spark.sql.connector.core.Constants
import org.apache.spark.unsafe.types.UTF8String

import java.lang.{Boolean => JBoolean, Double => JDouble, Float => JFloat, Long => JLong}
import java.time.temporal.ChronoUnit
import java.time.{Instant, OffsetDateTime}

/**
 * Converter that applies a transformation from a non-null object to a target type
 * @tparam T target type
 */

trait ReadTransformConverter[T]
  extends ReadConverter {

  override final def apply(value: Any): T = {

    Option(value) match {
      case Some(value) => transform(value)
      case None => null.asInstanceOf[T]
    }
  }

  /**
   * Convert the non-null Search object into a Spark internal object
   * @param value non-null value
   * @return an instance of target type
   */

  protected def transform(value: Any): T
}

/**
 * Converter that applies a simple casting to a non-null value
 * @tparam T target type
 */

final class ReadCastConverter[T]
  extends ReadTransformConverter[T] {
  override protected def transform(value: Any): T = value.asInstanceOf[T]
}

/**
 * Converter for date-time objects
 * @tparam T target type
 */

private sealed trait ReadTimeConverter[T]
  extends ReadTransformConverter[T] {

  override protected final def transform(value: Any): T = {

    // Convert to OffsetDateTime and then transform
    toInternalObject(
      OffsetDateTime.parse(value.asInstanceOf[String],
        Constants.DATETIME_OFFSET_FORMATTER
      )
    )
  }

  /**
   * Convert an instance of [[OffsetDateTime]] to a Spark internal object
   * @param dateTime offset date time
   * @return a Spark internal object
   */

  protected def toInternalObject(dateTime: OffsetDateTime): T
}

object ReadConverters {

  /**
   * Converter for reading numeric values as strings
   */

  final val STRING_VALUE_OF: ReadTransformConverter[String] = (value: Any) => value.asInstanceOf[String]

  /**
   * Converter for strings (internally represented by Spark as [[UTF8String]]s)
   */

  final val UTF8_STRING: ReadTransformConverter[UTF8String] = (value: Any) => UTF8String.fromString(value.asInstanceOf[String])

  final val INT32 = new ReadCastConverter[Integer]

  final val INT64 = new ReadCastConverter[JLong]

  final val DOUBLE = new ReadCastConverter[JDouble]

  final val SINGLE = new ReadCastConverter[JFloat]

  final val BOOLEAN = new ReadCastConverter[JBoolean]

  final val TIMESTAMP: ReadTimeConverter[JLong] = new ReadTimeConverter[JLong] {
    override protected def toInternalObject(dateTime: OffsetDateTime): JLong = {
      ChronoUnit.MICROS.between(Instant.EPOCH, dateTime.toInstant)
    }
  }

  final val DATE: ReadTimeConverter[Integer] = new ReadTimeConverter[Integer] {
    override protected def toInternalObject(dateTime: OffsetDateTime): Integer = {
      dateTime.toLocalDate.toEpochDay.toInt
    }
  }
}
