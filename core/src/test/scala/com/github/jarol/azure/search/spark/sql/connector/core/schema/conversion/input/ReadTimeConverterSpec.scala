package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input

import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, Constants}

import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalTime, OffsetDateTime}

class ReadTimeConverterSpec
  extends BasicSpec {

  describe(anInstanceOf[ReadTimeConverter[_]]) {
    describe(SHOULD) {
      describe("define a converter for time-based types, like") {
        it("timestamp") {

          val input: OffsetDateTime = OffsetDateTime.of(
            LocalDate.now(),
            LocalTime.now(),
            Constants.UTC_OFFSET
          )

          val expected: Long = ChronoUnit.MICROS.between(Instant.EPOCH, input.toInstant)
          ReadTimeConverter.TIMESTAMP.apply(input.format(Constants.DATETIME_OFFSET_FORMATTER)) shouldBe expected
          ReadTimeConverter.TIMESTAMP.apply(null.asInstanceOf[String]) shouldBe null
        }

        it("dates") {

          val input: OffsetDateTime = OffsetDateTime.of(
            LocalDate.now(),
            LocalTime.now(),
            Constants.UTC_OFFSET
          )

          val expected: Int = input.toLocalDate.toEpochDay.toInt
          ReadTimeConverter.DATE.apply(input.format(Constants.DATETIME_OFFSET_FORMATTER)) shouldBe expected
          ReadTimeConverter.DATE.apply(null.asInstanceOf[String]) shouldBe null
        }
      }
    }
  }
}
