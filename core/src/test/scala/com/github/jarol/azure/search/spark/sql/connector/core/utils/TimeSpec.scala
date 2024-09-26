package com.github.jarol.azure.search.spark.sql.connector.core.utils

import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, Constants}
import org.scalatest.TryValues

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime}
import scala.util.Success

class TimeSpec
  extends BasicSpec
    with TryValues {

  describe(`object`[Time.type ]) {
    describe(SHOULD) {
      describe(s"safely convert a string to an ${nameOf[OffsetDateTime]} when") {
        describe("it represents a timestamp") {

          val noMillisFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
          val (year, month, day, hour, minutes, second) = (2024, 9, 26, 12, 0, 0)
          val input = LocalDateTime.of(year, month, day, hour, minutes, second)

          it("without milliseconds") {

            Time.tryFromTimestamp(
              s"${input.format(noMillisFmt)}"
            ).success.value shouldBe input.atOffset(Constants.UTC_OFFSET)
          }

          it("with milliseconds") {

            Time.tryFromTimestamp(
              s"${input.format(noMillisFmt)}.000"
            ).success.value shouldBe input.atOffset(Constants.UTC_OFFSET)
          }

          it("with 'T' separating date from time") {

            val tFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
            Time.tryFromTimestamp(
              s"${input.format(tFmt)}.000"
            ).success.value shouldBe input.atOffset(Constants.UTC_OFFSET)
          }
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
