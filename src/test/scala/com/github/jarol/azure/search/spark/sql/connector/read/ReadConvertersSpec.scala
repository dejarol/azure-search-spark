package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.BasicSpec
import org.apache.spark.unsafe.types.UTF8String

import java.nio.charset.StandardCharsets
import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

class ReadConvertersSpec
  extends BasicSpec {

  private lazy val currentInstant = Instant.now()
  private lazy val currentDateTime = LocalDateTime.ofInstant(currentInstant, ZoneId.systemDefault())

  describe(`object`[ReadConverters.type ]) {
    describe(SHOULD) {
      describe("define converters for") {
        it("strings") {

          // [a] non-null input
          val input = "hello"
          val output = ReadConverters.StringConverter.apply(input)
          output shouldBe a [UTF8String]
          new String(output.getBytes, StandardCharsets.UTF_8) shouldBe input

          // [b] null input
          ReadConverters.StringConverter.apply(null.asInstanceOf[String]) shouldBe null
        }

        it("dates") {

          // [a] non-null input
          val output = ReadConverters.DateConverter.apply(currentDateTime.format(DateTimeFormatter.ISO_DATE_TIME))
          output shouldBe a[Int]
          output shouldBe ChronoUnit.DAYS.between(Instant.EPOCH, currentInstant)

          // [b] null input
          ReadConverters.DateConverter.apply(null.asInstanceOf[String]) shouldBe null
        }

        it("timestamps") {

          // [a] non-null input
          val output = ReadConverters.TimestampConverter.apply(currentDateTime.format(DateTimeFormatter.ISO_DATE_TIME))
          output shouldBe a[Long]
          output shouldBe ChronoUnit.MICROS.between(Instant.EPOCH, currentInstant)

          // [b] null input
          ReadConverters.TimestampConverter.apply(null.asInstanceOf[String]) shouldBe null
        }
      }
    }
  }
}
