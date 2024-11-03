package com.github.jarol.azure.search.spark.sql.connector

import com.github.jarol.azure.search.spark.sql.connector.core.Constants

import java.sql.{Date, Timestamp}
import java.time.{LocalTime, OffsetDateTime}
import java.util.{Map => JMap}
import java.lang.{Boolean => JBoolean, Double => JDouble, Long => JLong}
import scala.language.implicitConversions

/**
 * Collection of implicit methods and functions for easing read/write integration test
 */

package object models {

  /**
   * Convert a Java map to a [[MapOperations]] instance
   * @param map Java map
   * @return a [[MapOperations]] instance
   */

  implicit def toMapOperations(map: JMap[String, AnyRef]): MapOperations = new MapOperations(map)

  /**
   * Get the property serializer for a type having an implicit [[DocumentSerializer]] in scope
   * @tparam T serializer type
   * @return a property serializer for a type having an implicit [[DocumentSerializer]] in scope
   */

  implicit def documentSerializerOf[T: DocumentSerializer]: PropertySerializer[T] = {

    val docSerializer = implicitly[DocumentSerializer[T]]
    (v1: T) => docSerializer.serialize(v1)
  }

  /**
   * Serializer for strings
   */

  implicit object StringSerializer extends PropertySerializer[String] {
    override def serialize(v1: String): String = v1
  }

  /**
   * Deserializer for strings
   */

  implicit object StringDeserializer extends PropertyDeserializer[String] {
    override def deserialize(value: Any): String = value.asInstanceOf[String]
  }

  /**
   * Serializer for integers
   */

  implicit object IntSerializer extends PropertySerializer[Int] {
    override def serialize(v1: Int): Integer = v1
  }

  implicit object IntDeserializer extends PropertyDeserializer[Int] {
    override def deserialize(value: Any): Int = value.asInstanceOf[Integer]
  }

  /**
   * Serializer for longs
   */

  implicit object LongSerializer extends PropertySerializer[Long] {
    override def serialize(v1: Long): JLong = v1
  }

  /**
   * Serializer for double
   */

  implicit object DoubleSerializer extends PropertySerializer[Double] {
    override def serialize(v1: Double): JDouble = v1
  }

  /**
   * Serializer for boolean
   */

  implicit object BooleanSerializer extends PropertySerializer[Boolean] {
    override def serialize(v1: Boolean): JBoolean = v1
  }

  /**
   * Serializer for dates
   */

  implicit object DateSerializer extends PropertySerializer[Date] {
    override def serialize(v1: Date): String = {
      v1.toLocalDate.atTime(LocalTime.MIDNIGHT)
        .atOffset(Constants.UTC_OFFSET)
        .format(Constants.DATETIME_OFFSET_FORMATTER)
    }
  }

  /**
   * Deserializer for dates
   */

  implicit object DateDeserializer extends PropertyDeserializer[Date] {
    override def deserialize(value: Any): Date = {
      Date.valueOf(
        OffsetDateTime.parse(
          value.asInstanceOf[String],
          Constants.DATETIME_OFFSET_FORMATTER
        ).toLocalDate
      )
    }
  }

  /**
   * Serializer for timestamps
   */

  implicit object TimestampSerializer extends PropertySerializer[Timestamp] {
    override def serialize(v1: Timestamp): String = {
      v1.toInstant
        .atOffset(Constants.UTC_OFFSET)
        .format(Constants.DATETIME_OFFSET_FORMATTER)
    }
  }

  /**
   * Deserializer for timestamps
   */

  implicit object TimestampDeserializer extends PropertyDeserializer[Timestamp] {
    override def deserialize(value: Any): Timestamp = {
      Timestamp.from(
        OffsetDateTime.parse(
          value.asInstanceOf[String],
          Constants.DATETIME_OFFSET_FORMATTER
        ).toInstant
      )
    }
  }
}
