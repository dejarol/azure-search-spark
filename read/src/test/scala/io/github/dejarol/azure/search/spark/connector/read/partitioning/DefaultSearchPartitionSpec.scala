package io.github.dejarol.azure.search.spark.connector.read.partitioning

import io.github.dejarol.azure.search.spark.connector.core.BasicSpec

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
