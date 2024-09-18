package com.github.jarol.azure.search.spark.sql.connector.schema.conversion.output

import com.github.jarol.azure.search.spark.sql.connector.Constants
import org.apache.spark.unsafe.types.UTF8String

import java.nio.charset.StandardCharsets
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalTime, OffsetDateTime}

/**
 * Atomic converters from Spark internal objects to Search document properties
 */

object AtomicSearchConverters {

  /**
   * Converter for strings
   */

  case object StringConverter
    extends SearchPropertyTransformConverter[String] {
    override protected def transform(value: Any): String = {

      new String(
        value.asInstanceOf[UTF8String].getBytes,
        StandardCharsets.UTF_8
      )
    }
  }

  /**
   * Converter for integers
   */

  case object Int32Converter extends SearchPropertyCastConverter[java.lang.Integer]

  /**
   * Converter for longs
   */

  case object Int64Converter extends SearchPropertyCastConverter[java.lang.Long]

  /**
   * Converter for doubles
   */

  case object DoubleConverter extends SearchPropertyCastConverter[java.lang.Double]

  /**
   * Converter for floats
   */

  case object SingleConverter extends SearchPropertyCastConverter[java.lang.Float]

  /**
   * Converter for booleans
   */

  case object BooleanConverter extends SearchPropertyCastConverter[java.lang.Boolean]

  /**
   * Converter for dates
   */

  case object DateToDatetimeConverter
    extends SearchPropertyTimeConverter {
    override protected def toOffsetDateTime(value: Any): OffsetDateTime = {

      OffsetDateTime.of(
        LocalDate.ofEpochDay(value.asInstanceOf[Int].toLong),
        LocalTime.of(0, 0, 0, 0),
        Constants.UTC_OFFSET
      )
    }
  }

  /**
   * Converter for timestamps
   */

  case object TimestampToDatetimeConverter
    extends SearchPropertyTimeConverter {
    override protected def toOffsetDateTime(value: Any): OffsetDateTime = {

      Instant.EPOCH.plus(
        value.asInstanceOf[Long],
        ChronoUnit.MICROS
      ).atOffset(Constants.UTC_OFFSET)
    }
  }
}
