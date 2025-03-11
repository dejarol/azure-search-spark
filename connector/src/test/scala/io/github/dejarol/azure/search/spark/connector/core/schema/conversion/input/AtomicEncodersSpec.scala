package io.github.dejarol.azure.search.spark.connector.core.schema.conversion.input

import io.github.dejarol.azure.search.spark.connector.core.{BasicSpec, FieldFactory}
import org.apache.spark.unsafe.types.UTF8String

import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalTime, OffsetDateTime}

class AtomicEncodersSpec
  extends BasicSpec
    with FieldFactory {

  describe(`object`[AtomicEncoders]) {
    describe(SHOULD) {
      describe("provide a converter for") {
        describe("time-based types, like") {
          it("timestamp") {

            val input: OffsetDateTime = OffsetDateTime.of(
              LocalDate.now(),
              LocalTime.now(),
              Constants.UTC_OFFSET
            )

            val encoder = AtomicEncoders.forTimestamps()
            val expected: Long = ChronoUnit.MICROS.between(Instant.EPOCH, input.toInstant)
            encoder.apply(input.format(Constants.DATETIME_OFFSET_FORMATTER)) shouldBe expected
            encoder.apply(null.asInstanceOf[String]) shouldBe null
          }

          it("dates") {

            val input: OffsetDateTime = OffsetDateTime.of(
              LocalDate.now(),
              LocalTime.now(),
              Constants.UTC_OFFSET
            )

            val encoder = AtomicEncoders.forDates()
            val expected: Int = input.toLocalDate.toEpochDay.toInt
            encoder.apply(input.format(Constants.DATETIME_OFFSET_FORMATTER)) shouldBe expected
            encoder.apply(null.asInstanceOf[String]) shouldBe null
          }
        }

        describe("applying a transformation to a non-null Search objects, like") {
          it("normal strings") {

            val input = "hello"
            val encoder = AtomicEncoders.stringValueOf()
            encoder.apply(input) shouldBe input
            encoder.apply(null.asInstanceOf[String]) shouldBe null
          }

          it("UTF8 strings") {

            val input = "hello"
            val encoder = AtomicEncoders.forUTF8Strings()
            encoder.apply(input) shouldBe UTF8String.fromString(input)
            encoder.apply(null.asInstanceOf[String]) shouldBe null
          }
        }
      }
    }
  }
}
