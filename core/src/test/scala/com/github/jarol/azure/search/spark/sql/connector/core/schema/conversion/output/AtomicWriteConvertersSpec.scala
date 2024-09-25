package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output

import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, Constants}
import org.apache.spark.unsafe.types.UTF8String

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, OffsetDateTime}

class AtomicWriteConvertersSpec
  extends BasicSpec {

  import AtomicWriteConverters._

  describe(`object`[AtomicWriteConverters.type ]) {
    describe(SHOULD) {
      describe("provide a converter for") {
        it("strings") {

          val input = "hello"
          StringConverter.apply(
            UTF8String.fromString(input)
          ) shouldBe input

          StringConverter.apply(null) shouldBe null
        }

        it("int32") {

          val input: java.lang.Integer = 123
          Int32Converter.apply(input) shouldBe input
        }

        it("int64") {

          val input: java.lang.Long = 123
          Int64Converter.apply(input) shouldBe input
        }

        it("double") {

          val input: java.lang.Double = 3.14
          DoubleConverter.apply(input) shouldBe input
        }

        it("float") {

          val input: java.lang.Float = 3.14f
          SingleConverter.apply(input) shouldBe input
        }

        it("boolean") {

          val input = false
          BooleanConverter.apply(input) shouldBe input
        }

        it("date") {

          val input = OffsetDateTime.of(
            LocalDate.now(),
            LocalTime.MIDNIGHT,
            Constants.UTC_OFFSET
          )
          val expected = input.format(Constants.DATE_TIME_FORMATTER)
          DateToDatetimeConverter.apply(input.toLocalDate.toEpochDay.toInt) shouldBe expected
        }

        it("timestamp") {

          val input = OffsetDateTime.now(Constants.UTC_OFFSET)
          val expected = input.format(Constants.DATE_TIME_FORMATTER)
          TimestampToDatetimeConverter.apply(
            ChronoUnit.MICROS.between(Instant.EPOCH, input.toInstant)
          ) shouldBe expected
        }

        describe("converting strings to datetime offsets that handles") {

          it("datetimes with offset") {

            val input = OffsetDateTime.now(Constants.UTC_OFFSET)
              .format(Constants.DATE_TIME_FORMATTER)

            StringToDatetimeConverter.apply(input) shouldBe input
          }

          it("datetimes without offset") {

            val now = LocalDateTime.now()
            val input = now.format(Constants.DATE_TIME_FORMATTER)

            val expected = now.atOffset(Constants.UTC_OFFSET).format(Constants.DATE_TIME_FORMATTER)
            StringToDatetimeConverter.apply(input) shouldBe expected
          }

          it("dates") {

            val now = LocalDate.now()
            val input = now.format(DateTimeFormatter.ISO_LOCAL_DATE)
            val expected = OffsetDateTime
              .of(now, LocalTime.MIDNIGHT, Constants.UTC_OFFSET)
              .format(Constants.DATE_TIME_FORMATTER)

            StringToDatetimeConverter.apply(input) shouldBe expected
          }
        }

        it("long to int") {

          val input = 123
          val actual = LongToInt32Converter.apply(input.toLong)
          actual shouldBe a[java.lang.Integer]
          actual shouldBe input
        }

        it("int to long") {

          val input = 123
          val actual = IntegerToInt64Converter.apply(input)
          actual shouldBe a[java.lang.Long]
          actual shouldBe input.toLong
        }
      }
    }
  }
}
