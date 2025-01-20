package io.github.jarol.azure.search.spark.sql.connector

import io.github.jarol.azure.search.spark.sql.connector.core.Constants
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
   * @return a property serializer for a sub document of type T
   */

  implicit def documentSerializerOf[T: DocumentSerializer]: PropertySerializer[T] = {

    val docSerializer = implicitly[DocumentSerializer[T]]
    (v1: T) => docSerializer.serialize(v1)
  }

  /**
   * Get the property deserializer for a type having an implicit [[DocumentDeserializer]] in scope
   * @tparam T deserializer type
   * @return a property deserializer for a sub document of type T
   */

  implicit def documentDeserializerOf[T: DocumentDeserializer]: PropertyDeserializer[T] = {

    val deserializer = implicitly[DocumentDeserializer[T]]
    (value: Any) => deserializer.deserialize(
      value.asInstanceOf[JMap[String, AnyRef]]
    )
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

  /**
   * Deserializer for integers
   */

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
   * Deserializer for longs
   */

  implicit object LongDeserializer extends PropertyDeserializer[JLong] {
    override def deserialize(value: Any): JLong = {

      value match {
        case i: Integer => i.longValue()
        case l: JLong => l
      }
    }
  }

  /**
   * Serializer for double
   */

  implicit object DoubleSerializer extends PropertySerializer[Double] {
    override def serialize(v1: Double): JDouble = v1
  }

  /**
   * Deserializer for double
   */

  implicit object DoubleDeserializer extends PropertyDeserializer[Double] {
    override def deserialize(value: Any): Double = {
      value.asInstanceOf[Double]
    }
  }

  /**
   * Serializer for boolean
   */

  implicit object BooleanSerializer extends PropertySerializer[Boolean] {
    override def serialize(v1: Boolean): JBoolean = v1
  }

  /**
   * Deserializer for booleans
   */

  implicit object BooleanDeserializer extends PropertyDeserializer[Boolean] {
    override def deserialize(value: Any): Boolean = value.asInstanceOf[JBoolean]
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
