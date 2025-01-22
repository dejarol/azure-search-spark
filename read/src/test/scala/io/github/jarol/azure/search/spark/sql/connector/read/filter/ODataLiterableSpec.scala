package io.github.jarol.azure.search.spark.sql.connector.read.filter

import io.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, Constants}

import java.lang.{Double => JDouble, Long => JLong}
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

class ODataLiterableSpec
  extends BasicSpec {

  import ODataLiterables._

  describe(`object`[ODataLiterables.type ]) {
    describe(SHOULD) {
      describe("provide literables for") {
        it("strings") {

          val value = "hello"
          StringLiterable.toLiteral(value) shouldBe s"'$value'"
        }

        it("numbers") {

          numericLiterable[Integer].toLiteral(3) shouldBe "3"
          numericLiterable[JLong].toLiteral(456) shouldBe "456"
          numericLiterable[JDouble].toLiteral(3.14) shouldBe "3.14"
        }

        it("dates") {

          val value = LocalDate.now()
          DateLiterable.toLiteral(Date.valueOf(value)) shouldBe value
            .atStartOfDay(Constants.UTC_OFFSET)
            .format(Constants.DATETIME_OFFSET_FORMATTER)
        }

        it("timestamps") {

          val now = Instant.now()
          TimestampLiterable.toLiteral(
            Timestamp.from(now)
          ) shouldBe now.atOffset(Constants.UTC_OFFSET)
            .format(Constants.DATETIME_OFFSET_FORMATTER)
        }
      }
    }
  }
}
