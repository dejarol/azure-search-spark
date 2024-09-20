package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec

class FacetValuePartitionSpec
  extends BasicSpec {

  private lazy val fieldName = "fieldName"
  private lazy val fieldValue = "value"

  describe(anInstanceOf[FacetValuePartition]) {
    describe(SHOULD) {
      it("create a facet filter related to given value") {

        FacetValuePartition(0, None, None, fieldName, fieldValue).facetFilter shouldBe s"$fieldName eq $fieldValue"
      }
    }
  }
}
