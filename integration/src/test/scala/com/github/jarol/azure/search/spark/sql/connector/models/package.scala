package com.github.jarol.azure.search.spark.sql.connector

import com.github.jarol.azure.search.spark.sql.connector.core.Constants

import java.sql.{Date, Timestamp}
import java.time.LocalTime
import java.util.{Map => JMap}
import java.lang.{Boolean => JBoolean, Double => JDouble, Long => JLong}
import scala.language.implicitConversions

package object models {

  implicit def toMapOperations(map: JMap[String, AnyRef]): MapOperations = new MapOperations(map)

  implicit def documentSerializerOf[T: DocumentSerializer]: PropertySerializer[T] = {

    val docSerializer = implicitly[DocumentSerializer[T]]
    (v1: T) => docSerializer.serialize(v1)
  }

  implicit object StringSerializer$ extends PropertySerializer[String] {
    override def toPropertyValue(v1: String): String = v1
  }

  implicit object IntSerializer$ extends PropertySerializer[Int] {
    override def toPropertyValue(v1: Int): Integer = v1
  }

  implicit object LongSerializer$ extends PropertySerializer[Long] {
    override def toPropertyValue(v1: Long): JLong = v1
  }

  implicit object DoubleSerializer$ extends PropertySerializer[Double] {
    override def toPropertyValue(v1: Double): JDouble = v1
  }

  implicit object BooleanSerializer$ extends PropertySerializer[Boolean] {
    override def toPropertyValue(v1: Boolean): JBoolean = v1
  }

  implicit object DateSerializer$ extends PropertySerializer[Date] {
    override def toPropertyValue(v1: Date): String = {
      v1.toLocalDate.atTime(LocalTime.MIDNIGHT)
        .atOffset(Constants.UTC_OFFSET)
        .format(Constants.DATETIME_OFFSET_FORMATTER)
    }
  }

  implicit object TimestampSerializer$ extends PropertySerializer[Timestamp] {
    override def toPropertyValue(v1: Timestamp): String = {
      v1.toInstant
        .atOffset(Constants.UTC_OFFSET)
        .format(Constants.DATETIME_OFFSET_FORMATTER)
    }
  }
}
