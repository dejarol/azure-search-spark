package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output

import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, Constants}
import org.apache.spark.unsafe.types.UTF8String

import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalTime, OffsetDateTime}

class AtomicWriteConvertersSpec
  extends BasicSpec {

  import AtomicWriteConverters._

  describe(`object`[AtomicWriteConverters.type ]) {
    describe(SHOULD) {
      describe("provide a converter for") {
        it("strings") {

          val input = "hello"
          StringConverter.toSearchProperty(
            UTF8String.fromString(input)
          ) shouldBe input

          StringConverter.toSearchProperty(null) shouldBe null
        }

        it("int32") {

          val input: java.lang.Integer = 123
          Int32Converter.toSearchProperty(input) shouldBe input
        }

        it("int64") {

          val input: java.lang.Long = 123
          Int64Converter.toSearchProperty(input) shouldBe input
        }

        it("double") {

          val input: java.lang.Double = 3.14
          DoubleConverter.toSearchProperty(input) shouldBe input
        }

        it("float") {

          val input: java.lang.Float = 3.14f
          SingleConverter.toSearchProperty(input) shouldBe input
        }

        it("boolean") {

          val input = false
          BooleanConverter.toSearchProperty(input) shouldBe input
        }

        it("date") {

          val input = OffsetDateTime.of(
            LocalDate.now(),
            LocalTime.of(0, 0),
            Constants.UTC_OFFSET
          )
          val expected = input.format(Constants.DATE_TIME_FORMATTER)
          DateToDatetimeConverter.toSearchProperty(input.toLocalDate.toEpochDay.toInt) shouldBe expected
        }

        it("timestamp") {

          val input = OffsetDateTime.now(Constants.UTC_OFFSET)
          val expected = input.format(Constants.DATE_TIME_FORMATTER)
          TimestampToDatetimeConverter.toSearchProperty(
            ChronoUnit.MICROS.between(Instant.EPOCH, input.toInstant)
          ) shouldBe expected
        }
      }
    }
  }
}
