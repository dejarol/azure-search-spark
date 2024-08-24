package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import org.apache.spark.unsafe.types.UTF8String

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, OffsetDateTime}

object AtomicTypeConverters {

  case object StringConverter
    extends SparkInternalConverter {
    override def toSparkInternalObject(value: Any): UTF8String = {
      UTF8String.fromString(value.asInstanceOf[String])
    }
  }

  case object Int32Converter
    extends SparkInternalConverter {
    override def toSparkInternalObject(value: Any): java.lang.Integer = {
      value.asInstanceOf[java.lang.Integer]
    }
  }

  case object Int64Converter
    extends SparkInternalConverter {
    override def toSparkInternalObject(value: Any): java.lang.Long = {
      value.asInstanceOf[java.lang.Long]
    }
  }

  case object DoubleConverter
    extends SparkInternalConverter {
    override def toSparkInternalObject(value: Any): java.lang.Double = {
      value.asInstanceOf[java.lang.Double]
    }
  }

  case object SingleConverter
    extends SparkInternalConverter {
    override def toSparkInternalObject(value: Any): java.lang.Float = {
      value.asInstanceOf[java.lang.Float]
    }
  }

  case object BooleanConverter
    extends SparkInternalConverter {
    override def toSparkInternalObject(value: Any): java.lang.Boolean = {
      value.asInstanceOf[java.lang.Boolean]
    }
  }

  case object DateTimeToTimestampConverter
    extends SparkInternalConverter {
    override def toSparkInternalObject(value: Any): java.lang.Long = {

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
