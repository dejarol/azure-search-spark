package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output

import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, Constants}
import org.apache.spark.unsafe.types.UTF8String

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalTime, OffsetDateTime}

class AtomicDecodersSpec
  extends BasicSpec {

  describe(`object`[AtomicDecoders]) {
    describe(SHOULD) {
      describe("provide a decoder") {
        describe("for") {
          it("strings") {

            val input = "hello"
            val decoder = AtomicDecoders.forUTF8Strings()
            decoder.apply(UTF8String.fromString(input)) shouldBe input
            decoder.apply(null) shouldBe null
          }

          it("date") {

            val input = OffsetDateTime.of(
              LocalDate.now(),
              LocalTime.MIDNIGHT,
              Constants.UTC_OFFSET
            )
            val expected = input.format(Constants.DATETIME_OFFSET_FORMATTER)
            AtomicDecoders.forDates().apply(input.toLocalDate.toEpochDay.toInt) shouldBe expected
          }

          it("timestamp") {

            val input = OffsetDateTime.now(Constants.UTC_OFFSET)
            val expected = input.format(Constants.DATETIME_OFFSET_FORMATTER)
            AtomicDecoders.forTimestamps().apply(
              ChronoUnit.MICROS.between(Instant.EPOCH, input.toInstant)
            ) shouldBe expected
          }
        }

        describe("from") {
          describe("date to") {
            it("strings") {

              val input = LocalDate.now()
              AtomicDecoders.fromDateToString().apply(
                input.toEpochDay.toInt
              ) shouldBe input.format(DateTimeFormatter.ISO_LOCAL_DATE)
            }
          }
        }
      }
    }
  }
}
