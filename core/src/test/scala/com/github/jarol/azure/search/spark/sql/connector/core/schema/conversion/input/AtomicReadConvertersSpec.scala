package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input

import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, Constants}
import org.apache.spark.unsafe.types.UTF8String

import java.time._
import java.time.temporal.ChronoUnit

class AtomicReadConvertersSpec
  extends BasicSpec {

  import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input.AtomicReadConverters._

  describe("Each atomic type converter") {
    describe(SHOULD) {
      describe("convert a Search object into a Spark internal object") {
        it("string") {

          val input = "hello"
          StringConverter.apply(input) shouldBe UTF8String.fromString(input)
          StringConverter.apply(null.asInstanceOf[String]) shouldBe null
        }

        it("int") {

          val input: Integer = 23
          Int32Converter.apply(input) shouldBe input
          Int32Converter.apply(null.asInstanceOf[Integer]) shouldBe null
        }

        it("long") {

          val input: java.lang.Long = 23
          Int64Converter.apply(input) shouldBe input
          Int64Converter.apply(null.asInstanceOf[java.lang.Long]) shouldBe null
        }

        it("double") {

          val input: java.lang.Double = 3.14
          DoubleConverter.apply(input) shouldBe input
          DoubleConverter.apply(null.asInstanceOf[java.lang.Long]) shouldBe null
        }

        it("float") {

          val input: java.lang.Float = 3.14f
          SingleConverter.apply(input) shouldBe input
          SingleConverter.apply(null.asInstanceOf[java.lang.Long]) shouldBe null
        }

        it("boolean") {

          val input: java.lang.Boolean = false
          BooleanConverter.apply(input) shouldBe input
          BooleanConverter.apply(null.asInstanceOf[java.lang.Boolean]) shouldBe null
        }

        it("timestamp") {

          val input: OffsetDateTime = OffsetDateTime.of(
            LocalDate.now(),
            LocalTime.now(),
            Constants.UTC_OFFSET
          )

          val expected: Long = ChronoUnit.MICROS.between(Instant.EPOCH, input.toInstant)
          DateTimeToTimestampConverter.apply(input.format(Constants.DATE_TIME_FORMATTER)) shouldBe expected
          DateTimeToTimestampConverter.apply(null.asInstanceOf[String]) shouldBe null
        }

        it("dates") {

          val input: OffsetDateTime = OffsetDateTime.of(
            LocalDate.now(),
            LocalTime.now(),
            Constants.UTC_OFFSET
          )

          val expected: Int = input.toLocalDate.toEpochDay.toInt
          DateTimeToDateConverter.apply(input.format(Constants.DATE_TIME_FORMATTER)) shouldBe expected
          DateTimeToDateConverter.apply(null.asInstanceOf[String]) shouldBe null
        }

        it("int to long") {

          val input = 123
          val actual = Int32ToLongConverter.apply(input)
          actual shouldBe a[java.lang.Long]
          actual shouldBe input.toLong
        }

        it("long to int") {

          val input = 123
          val actual = Int64ToIntConverter.apply(input.toLong)
          actual shouldBe a[java.lang.Integer]
          actual shouldBe input
        }
      }
    }
  }
}
