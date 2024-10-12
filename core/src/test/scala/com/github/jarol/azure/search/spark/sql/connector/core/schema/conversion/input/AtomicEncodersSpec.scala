package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input

import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, Constants, FieldFactory}
import org.apache.spark.unsafe.types.UTF8String

import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalTime, OffsetDateTime}

class AtomicEncodersSpec
  extends BasicSpec
    with FieldFactory {

  describe(`object`[AtomicEncoders]) {
    describe(SHOULD) {
      describe("provide a converter for") {
        /*
        describe("casting a non-null Search object into a Spark internal type, like") {
          it("int") {

            val input: Integer = 23
            AtomicEncoders.INT32.apply(input) shouldBe input
            AtomicEncoders.INT32.apply(null.asInstanceOf[Integer]) shouldBe null
          }

          it("long") {

            val input: java.lang.Long = 23
            AtomicEncoders.INT64.apply(input) shouldBe input
            AtomicEncoders.INT64.apply(null.asInstanceOf[java.lang.Long]) shouldBe null
          }

          it("double") {

            val input: java.lang.Double = 3.14
            AtomicEncoders.DOUBLE.apply(input) shouldBe input
            AtomicEncoders.DOUBLE.apply(null.asInstanceOf[java.lang.Long]) shouldBe null
          }

          it("float") {

            val input: java.lang.Float = 3.14f
            AtomicEncoders.SINGLE.apply(input) shouldBe input
            AtomicEncoders.SINGLE.apply(null.asInstanceOf[java.lang.Long]) shouldBe null
          }

          it("boolean") {

            val input: java.lang.Boolean = false
            AtomicEncoders.BOOLEAN.apply(input) shouldBe input
            AtomicEncoders.BOOLEAN.apply(null.asInstanceOf[java.lang.Boolean]) shouldBe null
          }
        }
        
         */

        describe("time-based types, like") {
          it("timestamp") {

            val input: OffsetDateTime = OffsetDateTime.of(
              LocalDate.now(),
              LocalTime.now(),
              Constants.UTC_OFFSET
            )

            val expected: Long = ChronoUnit.MICROS.between(Instant.EPOCH, input.toInstant)
            AtomicEncoders.TIMESTAMP.apply(input.format(Constants.DATETIME_OFFSET_FORMATTER)) shouldBe expected
            AtomicEncoders.TIMESTAMP.apply(null.asInstanceOf[String]) shouldBe null
          }

          it("dates") {

            val input: OffsetDateTime = OffsetDateTime.of(
              LocalDate.now(),
              LocalTime.now(),
              Constants.UTC_OFFSET
            )

            val expected: Int = input.toLocalDate.toEpochDay.toInt
            AtomicEncoders.DATE.apply(input.format(Constants.DATETIME_OFFSET_FORMATTER)) shouldBe expected
            AtomicEncoders.DATE.apply(null.asInstanceOf[String]) shouldBe null
          }
        }

        describe("applying a transformation to a non-null Search objects, like") {
          it("normal strings") {

            val input = "hello"
            AtomicEncoders.STRING_VALUE_OF.apply(input) shouldBe input
            AtomicEncoders.STRING_VALUE_OF.apply(null.asInstanceOf[String]) shouldBe null
          }

          it("UTF8 strings") {

            val input = "hello"
            AtomicEncoders.UTF8_STRING.apply(input) shouldBe UTF8String.fromString(input)
            AtomicEncoders.UTF8_STRING.apply(null.asInstanceOf[String]) shouldBe null
          }
        }
      }
    }
  }
}
