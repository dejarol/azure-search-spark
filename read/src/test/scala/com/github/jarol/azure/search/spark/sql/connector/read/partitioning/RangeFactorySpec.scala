package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, OffsetDateTime}

class RangeFactorySpec
  extends BasicSpec {

  describe(anInstanceOf[RangeFactory[_, _]]) {
    describe(SHOULD) {
      describe("create a range of types values") {
        it("for integers") {

          val (lower, upper, partitions) = (1, 21, 2)
          val bounds = RangeFactory.Int.createRange(lower, upper, partitions)
          bounds.head shouldBe lower
          bounds.last should be <= upper
          bounds shouldBe sorted
          forAll(bounds.tail.dropRight(1)) {
            v =>
              v should be > lower
              v should be < upper
          }
        }

        it("for dates") {

          val (lower, upper, partitions) = (OffsetDateTime.now().minusWeeks(1), OffsetDateTime.now(), 10)
          val bounds = RangeFactory.Date.createRange(lower, upper, partitions)
          bounds.head shouldBe lower
          bounds.last should be <= upper
          bounds shouldBe sorted
          forAll(bounds.tail.dropRight(1)) {
            v =>
              v should be > lower
              v should be < upper
          }
        }

        it("for doubles") {

          val (lower, upper, partitions) = (1.0, 20.0, 2)
          val bounds = RangeFactory.Double.createRange(lower, upper, partitions)
          bounds.head shouldBe lower
          bounds.last should be <= upper
          bounds shouldBe sorted
          forAll(bounds.tail.dropRight(1)) {
            v =>
              v should be > lower
              v should be < upper
          }
        }
      }

      describe("provide a range only when") {
        it("all conditions hold") {

          RangeFactory.Int.createPartitionBounds(
            String.valueOf(1),
            String.valueOf(10),
            5
          ) shouldBe 'right
        }

        describe("for dates") {
          it("two dates are given") {

            val upper = LocalDate.now()
            RangeFactory.Date.createPartitionBounds(
              upper.minusMonths(1).format(DateTimeFormatter.ISO_LOCAL_DATE),
              upper.format(DateTimeFormatter.ISO_LOCAL_DATE),
              7
            ) shouldBe 'right
          }
        }
      }
    }

    describe(SHOULD_NOT) {
      describe("create a range of string values when") {
        it("lower bound is not valid") {

          RangeFactory.Int.createPartitionBounds(
            "hello",
            String.valueOf(10),
            3
          ) shouldBe 'left
        }

        it("upper bound is not valid") {

          RangeFactory.Int.createPartitionBounds(
            String.valueOf(1),
            "hello",
            3
          ) shouldBe 'left
        }

        it("lower bound is greater than upper bound") {

          RangeFactory.Int.createPartitionBounds(
            String.valueOf(10),
            String.valueOf(1),
            3
          ) shouldBe 'left
        }

        it("num partitions is lower than 2") {

          RangeFactory.Int.createPartitionBounds(
            String.valueOf(1),
            String.valueOf(10),
            1
          ) shouldBe 'left
        }
      }
    }
  }
}
