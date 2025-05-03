package io.github.dejarol.azure.search.spark.connector.read.partitioning

import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.{BasicSpec, FieldFactory}

class FacetedPartitionerSpec
  extends BasicSpec
    with FieldFactory {

  describe(anInstanceOf[FacetedPartitioner]) {
    describe(SHOULD) {
      it("create a collection of partitions") {

        val facetFieldName = "field"
        val facetValues = Seq("v1", "v2", "v3")
        val actual = JavaScalaConverters.listToSeq(
          FacetedPartitioner(facetFieldName, facetValues)
            .createPartitions()
        )

        actual should have size(facetValues.size + 1)
        actual.count {
          _.isInstanceOf[FacetValuePartition]
        } shouldBe facetValues.size

        actual.count {
          _.isInstanceOf[FacetNullValuePartition]
        } shouldBe 1
      }
    }
  }
}
