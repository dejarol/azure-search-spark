package com.github.jarol.azure.search.spark.sql.connector.core.utils

import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, Constants}
import org.scalatest.TryValues

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime}

class TimeSpec
  extends BasicSpec
    with TryValues {

  describe(`object`[Time.type ]) {
    describe(SHOULD) {
      describe(s"safely convert a string to an ${nameOf[OffsetDateTime]} when") {
        it("it has on offset") {

          val input = OffsetDateTime.now(Constants.UTC_OFFSET)
          Time.tryFromOffsetDateTime(
            input.format(Constants.DATE_TIME_FORMATTER)
          ).success.value shouldBe input

        }

        it("it does not have any offset") {

          val input = LocalDateTime.now()
          Time.tryFromDateTimeWithoutOffset(
            input.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
          ).success.value shouldBe input.atOffset(Constants.UTC_OFFSET)
        }

        it("it's a simple date") {

          val input = LocalDate.now()
          Time.tryFromDate(
            input.format(DateTimeFormatter.ISO_LOCAL_DATE)
          ).success.value shouldBe OffsetDateTime.of(
            input,
            LocalTime.MIDNIGHT,
            Constants.UTC_OFFSET
          )
        }
      }
    }
  }
}
