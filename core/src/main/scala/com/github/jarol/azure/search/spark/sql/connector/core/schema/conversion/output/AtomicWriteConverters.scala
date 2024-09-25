package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output

import com.github.jarol.azure.search.spark.sql.connector.core.Constants
import com.github.jarol.azure.search.spark.sql.connector.core.utils.Time
import org.apache.spark.unsafe.types.UTF8String

import java.lang
import java.nio.charset.StandardCharsets
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalTime, OffsetDateTime}

/**
 * Atomic converters from Spark internal objects to Search document properties
 */

object AtomicWriteConverters {

  /**
   * Converter for strings
   */

  case object StringConverter
    extends WriteTransformConverter[String] {
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

  case object Int32Converter extends WriteCastConverter[lang.Integer]

  /**
   * Converter for longs
   */

  case object Int64Converter extends WriteCastConverter[lang.Long]

  /**
   * Converter for doubles
   */

  case object DoubleConverter extends WriteCastConverter[lang.Double]

  /**
   * Converter for floats
   */

  case object SingleConverter extends WriteCastConverter[lang.Float]

  /**
   * Converter for booleans
   */

  case object BooleanConverter extends WriteCastConverter[lang.Boolean]

  /**
   * Converter for dates
   */

  case object DateToDatetimeConverter
    extends WriteTimeConverter {
    override protected def toOffsetDateTime(value: Any): OffsetDateTime = {

      OffsetDateTime.of(
        LocalDate.ofEpochDay(value.asInstanceOf[Int].toLong),
        LocalTime.MIDNIGHT,
        Constants.UTC_OFFSET
      )
    }
  }

  /**
   * Converter for timestamps
   */

  case object TimestampToDatetimeConverter
    extends WriteTimeConverter {
    override protected def toOffsetDateTime(value: Any): OffsetDateTime = {

      Instant.EPOCH.plus(
        value.asInstanceOf[Long],
        ChronoUnit.MICROS
      ).atOffset(Constants.UTC_OFFSET)
    }
  }

  case object StringToDatetimeConverter
    extends WriteTransformConverter[String] {

    override protected def transform(value: Any): String = {

      val string = value.asInstanceOf[String]
      Time.safelyToOffsetDatetime(string).toOption
        .map(_.format(Constants.DATE_TIME_FORMATTER))
        .orNull
    }
  }

  case object LongToInt32Converter
    extends WriteTransformConverter[lang.Integer] {

    override protected def transform(value: Any): Integer = {
      value.asInstanceOf[lang.Long].intValue()
    }
  }

  case object IntegerToInt64Converter
    extends WriteTransformConverter[lang.Long] {

    override protected def transform(value: Any): lang.Long = {
      value.asInstanceOf[lang.Integer].longValue()
    }
  }
}
