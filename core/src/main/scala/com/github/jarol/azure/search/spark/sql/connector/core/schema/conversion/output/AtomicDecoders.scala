package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output

import com.github.jarol.azure.search.spark.sql.connector.core.Constants
import org.apache.spark.unsafe.types.UTF8String

import java.nio.charset.StandardCharsets
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalTime, OffsetDateTime}

/**
 * Converter that applies a transformation to a non-null Spark internal value
 * @tparam T Search target type
 */

trait TransformDecoder[T]
  extends SearchDecoder {

  override final def apply(value: Any): T = {

    Option(value)
      .map(transform)
      .getOrElse(null.asInstanceOf[T])
  }

  /**
   * Apply the transformation
   *
   * @param value Spark internal value
   * @return a Search document property
   */

  protected def transform(value: Any): T
}

/**
 * Converter from Spark internal time types to Search datetime type
 */

sealed trait TimeDecoder
  extends TransformDecoder[String] {

  override final protected def transform(value: Any): String = {

    // Convert the Spark internal object to date time and then format it as a string
    toOffsetDateTime(value).format(
      Constants.DATETIME_OFFSET_FORMATTER
    )
  }

  /**
   * Transform the Spark internal value to an [[OffsetDateTime]]
   * @param value Spark internal value
   * @return an offset date time
   */

  protected def toOffsetDateTime(value: Any): OffsetDateTime
}


object AtomicDecoders {

  final val STRING_VALUE_OF: TransformDecoder[String] = (value: Any) => String.valueOf(value)

  final val STRING: TransformDecoder[String] = (value: Any) => new String(value.asInstanceOf[UTF8String].getBytes, StandardCharsets.UTF_8)

  /**
   * Converter for dates
   */

  final val DATE: TimeDecoder = new TimeDecoder {
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

  final val TIMESTAMP: TimeDecoder = new TimeDecoder {
    override protected def toOffsetDateTime(value: Any): OffsetDateTime = {

      Instant.EPOCH.plus(
        value.asInstanceOf[Long],
        ChronoUnit.MICROS
      ).atOffset(Constants.UTC_OFFSET)
    }
  }
}
