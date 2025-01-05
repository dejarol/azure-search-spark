package com.github.jarol.azure.search.spark.sql.connector.core.utils

import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, Constants}

import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalTime, OffsetDateTime}

class TimeUtilsSpec
  extends BasicSpec {

  describe(`object`[TimeUtils]) {
    describe(SHOULD) {
      describe(s"provide methods for getting an ${nameOf[OffsetDateTime]}") {
        it("from epoch days") {

          val input = LocalDate.now()
          val actual = TimeUtils.fromEpochDays(input.toEpochDay.toInt)
          actual.toLocalDate shouldBe input
          actual.toLocalTime shouldBe LocalTime.MIDNIGHT
        }

        it("from epoch micros") {

          val input = Instant.now()
          val actual = TimeUtils.fromEpochMicros(ChronoUnit.MICROS.between(Instant.EPOCH, input))
          actual shouldBe OffsetDateTime.ofInstant(input, Constants.UTC_OFFSET)
        }
      }
    }
  }
}
