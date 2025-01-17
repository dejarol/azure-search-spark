package com.github.jarol.azure.search.spark.sql.connector.core.utils

import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, Constants}

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalTime, OffsetDateTime}

class TimeUtilsSpec
  extends BasicSpec {

  describe(`object`[TimeUtils]) {
    describe(SHOULD) {
      describe(s"provide methods for getting an ${nameOf[OffsetDateTime]} from") {
        it("epoch days") {

          val input = LocalDate.now()
          val actual = TimeUtils.offsetDateTimeFromEpochDays(input.toEpochDay.toInt)
          actual.toLocalDate shouldBe input
          actual.toLocalTime shouldBe LocalTime.MIDNIGHT
        }

        it("epoch micros") {

          val input = Instant.now()
          val actual = TimeUtils.offsetDateTimeFromEpochMicros(ChronoUnit.MICROS.between(Instant.EPOCH, input))
          actual shouldBe OffsetDateTime.ofInstant(input, Constants.UTC_OFFSET)
        }

        it("a local date with pattern yyyy-MM-dd") {

          val input = LocalDate.now()
          val actual = TimeUtils.offsetDateTimeFromLocalDate(
            input.format(DateTimeFormatter.ISO_LOCAL_DATE)
          )

          actual.toLocalDate shouldBe input
          actual.toLocalTime shouldBe LocalTime.MIDNIGHT
          actual.getOffset shouldBe Constants.UTC_OFFSET
        }
      }
    }
  }
}
