package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec

class RangePartitionSpec
  extends BasicSpec {

  describe(anInstanceOf[RangePartition]) {
    describe(SHOULD) {
      describe("generate a filter that combines the 3 sub filters") {
        it("a") {

          // TODO
        }
      }
    }
  }

  describe(`object`[RangePartition]) {
    describe(SHOULD) {
      it("create a collection of partitions") {

        // TODO
      }
    }
  }
}
