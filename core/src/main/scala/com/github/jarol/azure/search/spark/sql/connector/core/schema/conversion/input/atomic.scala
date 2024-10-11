package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input

import com.github.jarol.azure.search.spark.sql.connector.core.Constants
import org.apache.spark.unsafe.types.UTF8String

import java.lang.{Long => JLong, Object => JObject}
import java.time.temporal.ChronoUnit
import java.time.{Instant, OffsetDateTime}

/**
 * Converter that applies a transformation from a non-null object to a target type
 * @tparam T target type
 */

trait TransformEncoder[T]
  extends SearchEncoder {

  override final def apply(value: JObject): T = {

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

  protected def transform(value: JObject): T
}

trait ReadTimeConverter[T]
  extends TransformEncoder[T] {

  override protected final def transform(value: JObject): T = {

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

object AtomicEncoders {

  /**
   * Encoder from numeric/boolean types to strings
   */

  final val STRING_VALUE_OF: TransformEncoder[String] = (value: JObject) => value.asInstanceOf[String]

  /**
   * Encoder for strings (internally represented by Spark as [[UTF8String]]s)
   */

  final val UTF8_STRING: TransformEncoder[UTF8String] = (value: JObject) => UTF8String.fromString(value.asInstanceOf[String])

  /**
   * Encoder for timestamps
   */

  final val TIMESTAMP: ReadTimeConverter[JLong] = (dateTime: OffsetDateTime) => {
    ChronoUnit.MICROS.between(Instant.EPOCH, dateTime.toInstant)
  }

  /**
   * Encoder for dates
   */

  final val DATE: ReadTimeConverter[Integer] = (dateTime: OffsetDateTime) => {
    dateTime.toLocalDate.toEpochDay.toInt
  }
}
