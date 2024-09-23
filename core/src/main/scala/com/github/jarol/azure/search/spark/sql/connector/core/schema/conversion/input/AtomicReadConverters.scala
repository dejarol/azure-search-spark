package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input

import org.apache.spark.unsafe.types.UTF8String

import java.lang
import java.time.temporal.ChronoUnit
import java.time.{Instant, OffsetDateTime}

/**
 * Converters for Search atomic types
 */

object AtomicReadConverters {

  /**
   * Converter for strings (internally represented by [[UTF8String]]s)
   */

  case object StringConverter
    extends ReadTransformConverter[UTF8String] {

    override protected def transform(value: Any): UTF8String = {
      UTF8String.fromString(value.asInstanceOf[String])
    }
  }

  /**
   * Converter for integers
   */

  case object Int32Converter extends ReadCastConverter[java.lang.Integer]

  /**
   * Converter for longs
   */

  case object Int64Converter extends ReadCastConverter[java.lang.Long]

  /**
   * Converter for doubles
   */

  case object DoubleConverter extends ReadCastConverter[java.lang.Double]

  /**
   * Converter for floats
   */

  case object SingleConverter extends ReadCastConverter[java.lang.Float]

  /**
   * Converter for booleans
   */

  case object BooleanConverter extends ReadCastConverter[java.lang.Boolean]

  /**
   * Converter for timestamp (internally represented as microsecond since epoch)
   */

  case object DateTimeToTimestampConverter
    extends ReadTimeConverter[java.lang.Long] {

    override protected def dateTimeToInternalObject(dateTime: OffsetDateTime): lang.Long = {

      ChronoUnit.MICROS.between(
        Instant.EPOCH,
        dateTime.toInstant
      )
    }
  }

  /**
   * Converter for dates (internally represented as epoch days)
   */

  case object DateTimeToDateConverter
    extends ReadTimeConverter[Integer] {

    override protected def dateTimeToInternalObject(dateTime: OffsetDateTime): Integer = {
      dateTime.toLocalDate.toEpochDay.toInt
    }
  }
}
