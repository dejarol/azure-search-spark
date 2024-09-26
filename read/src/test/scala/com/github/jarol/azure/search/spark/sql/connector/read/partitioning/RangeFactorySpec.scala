package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import org.scalatest.Inspectors

class RangeFactorySpec
  extends BasicSpec
    with Inspectors {

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

        describe("for dates when") {
          it("a date is provided") {
            // TODO: test
          }

          it("a timestamp is provided") {
            // TODO: test
          }
        }

        it("for doubles") {

          // TODO: test
        }
      }
    }
  }
}
