package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.github.jarol.azure.search.spark.sql.connector.BasicSpec
import org.apache.spark.unsafe.types.UTF8String

import java.time.temporal.ChronoUnit
import java.time._

class AtomicTypeConvertersSpec
  extends BasicSpec {

  import AtomicTypeConverters._

  describe("Each atomic type converter") {
    describe(SHOULD) {
      describe("convert a Search object into a Spark internal object") {
        it("string") {

          val input = "hello"
          StringConverter.toSparkInternalObject(input) shouldBe UTF8String.fromString(input)
          StringConverter.toSparkInternalObject(null.asInstanceOf[String]) shouldBe null
        }

        it("int") {

          val input: Integer = 23
          Int32Converter.toSparkInternalObject(input) shouldBe input
          Int32Converter.toSparkInternalObject(null.asInstanceOf[Integer]) shouldBe null
        }

        it("long") {

          val input: java.lang.Long = 23
          Int64Converter.toSparkInternalObject(input) shouldBe input
          Int64Converter.toSparkInternalObject(null.asInstanceOf[java.lang.Long]) shouldBe null
        }

        it("double") {

          val input: java.lang.Double = 3.14
          DoubleConverter.toSparkInternalObject(input) shouldBe input
          DoubleConverter.toSparkInternalObject(null.asInstanceOf[java.lang.Long]) shouldBe null
        }

        it("float") {

          val input: java.lang.Float = 3.14f
          SingleConverter.toSparkInternalObject(input) shouldBe input
          SingleConverter.toSparkInternalObject(null.asInstanceOf[java.lang.Long]) shouldBe null
        }

        it("boolean") {

          val input: java.lang.Boolean = false
          BooleanConverter.toSparkInternalObject(input) shouldBe input
          BooleanConverter.toSparkInternalObject(null.asInstanceOf[java.lang.Boolean]) shouldBe null
        }

        it("timestamp") {

          val input: OffsetDateTime = OffsetDateTime.of(
            LocalDate.now(),
            LocalTime.now(),
            ZoneOffset.UTC
          )

          val expected: Long = ChronoUnit.MICROS.between(Instant.EPOCH, input.toInstant)
          DateTimeToTimestampConverter.toSparkInternalObject(input.format(SparkInternalTimeConverter.SEARCH_DATE_FORMATTER)) shouldBe expected
          DateTimeToTimestampConverter.toSparkInternalObject(null.asInstanceOf[String]) shouldBe null
        }

        it("dates") {

          val input: OffsetDateTime = OffsetDateTime.of(
            LocalDate.now(),
            LocalTime.now(),
            ZoneOffset.UTC
          )

          val expected: Int = input.toLocalDate.toEpochDay.toInt
          DateTimeToDateConverter.toSparkInternalObject(input.format(SparkInternalTimeConverter.SEARCH_DATE_FORMATTER)) shouldBe expected
          DateTimeToDateConverter.toSparkInternalObject(null.asInstanceOf[String]) shouldBe null
        }
      }
    }
  }
}
