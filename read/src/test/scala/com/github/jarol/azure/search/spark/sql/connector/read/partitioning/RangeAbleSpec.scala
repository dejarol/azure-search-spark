package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, Constants}

import java.time.{LocalDate, LocalTime, OffsetDateTime}

class RangeAbleSpec
  extends BasicSpec {

  describe("a") {
    describe(SHOULD) {
      it("a") {

        println(RangeAble.Date.computeBounds(
          OffsetDateTime.of(
            LocalDate.now(),
            LocalTime.MIDNIGHT,
            Constants.UTC_OFFSET
          ),
          OffsetDateTime.now(Constants.UTC_OFFSET),
          2
        ))
      }
    }
  }
}
