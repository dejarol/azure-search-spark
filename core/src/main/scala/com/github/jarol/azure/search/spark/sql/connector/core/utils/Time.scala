package com.github.jarol.azure.search.spark.sql.connector.core.utils

import com.github.jarol.azure.search.spark.sql.connector.core.Constants

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime}
import scala.util.Try

object Time {

  /**
   * Try to convert a string that represents a zoned date time to a [[OffsetDateTime]]
   * @param value input value
   * @return a non-empty [[Option]] if given string can be parsed as a [[OffsetDateTime]]
   */

  final def tryFromOffsetDateTime(value: String): Try[OffsetDateTime] = {

    Try {
      OffsetDateTime
        .parse(value, Constants.DATE_TIME_FORMATTER)
    }
  }

  /**
   * Try to convert a string that represents an un-zoned date time to a [[OffsetDateTime]]
   * @param value input value
   * @return a non-empty [[Option]] if given string can be parsed as a [[OffsetDateTime]]
   */

  final def tryFromDateTimeWithoutOffset(value: String): Try[OffsetDateTime] = {

    Try {
      LocalDateTime
        .parse(value, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        .atOffset(Constants.UTC_OFFSET)
    }
  }

  /**
   * Try to convert a string that represents a date to a [[OffsetDateTime]]
   * @param value input value
   * @return a non-empty [[Option]] if given string can be parsed as a [[OffsetDateTime]]
   */

  final def tryFromDate(value: String): Try[OffsetDateTime] = {

    Try {
      OffsetDateTime.of(
        LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE),
        LocalTime.MIDNIGHT,
        Constants.UTC_OFFSET
      )
    }
  }

  final def safelyToOffsetDatetime(value: String): Try[OffsetDateTime] = {

    tryFromOffsetDateTime(value)
      .orElse(tryFromDateTimeWithoutOffset(value))
      .orElse(tryFromDate(value))
  }
}
