package io.github.jarol.azure.search.spark.sql.connector.read.partitioning

import io.github.jarol.azure.search.spark.sql.connector.core.BasicSpec

class DefaultSearchPartitionSpec
  extends BasicSpec {

  describe(anInstanceOf[DefaultSearchPartition]) {
    describe(SHOULD_NOT) {
      it("define any partition filter") {

        DefaultSearchPartition(0).getPartitionFilter shouldBe null
      }
    }
  }
}
