package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import org.apache.spark.unsafe.types.UTF8String

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, OffsetDateTime}

object AtomicTypeConverters {

  case object StringConverter
    extends SparkInternalTransformConverter[UTF8String] {
    override protected def transform(value: Any): UTF8String = {
      UTF8String.fromString(value.asInstanceOf[String])
    }
  }

  case object Int32Converter extends SparkInternalCastConverter[java.lang.Integer]
  case object Int64Converter extends SparkInternalCastConverter[java.lang.Long]
  case object DoubleConverter extends SparkInternalCastConverter[java.lang.Double]
  case object SingleConverter extends SparkInternalCastConverter[java.lang.Float]
  case object BooleanConverter extends SparkInternalCastConverter[java.lang.Boolean]

  case object DateTimeToTimestampConverter
    extends SparkInternalTransformConverter[java.lang.Long] {
    override protected def transform(value: Any): java.lang.Long = {

      // Convert string datetime to instant
      val datetimeInstant: Instant = OffsetDateTime.parse(
        value.asInstanceOf[String],
        DateTimeFormatter.ISO_DATE_TIME
      ).toInstant

      // Compute number of microseconds since epoch
      ChronoUnit.MICROS.between(
        Instant.EPOCH,
        datetimeInstant
      )
    }
  }
}
