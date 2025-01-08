package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}

class AbstractFacetPartitionSpec
  extends BasicSpec
    with FieldFactory {

  private lazy val defaultFacetFilter = "field eq value"

  /**
   * Create a partition instance
   * @return a partition instance
   */

  private def createPartition(): AbstractFacetPartition = {

    new AbstractFacetPartition(
      0,
      None,
      None,
      Seq.empty,
      "facetFieldName"
    ) {

      override def facetFilter: String = defaultFacetFilter
    }
  }

  describe(anInstanceOf[AbstractFacetPartition]) {
    describe(SHOULD) {
      describe("create a partition filter") {
        it("that simply contains the facet filter") {

          val partition = createPartition()
          partition.facetFilter shouldBe defaultFacetFilter
          partition.partitionFilter shouldBe Some(defaultFacetFilter)
        }
      }
    }
  }

  describe(`object`[AbstractFacetPartition]) {
    describe(SHOULD) {
      it("create a collection of partitions") {

        val facetFieldName = "field"
        val facetValues = Seq("v1", "v2", "v3")
        val actual = AbstractFacetPartition.createCollection(
          None,
          None,
          Seq.empty,
          createSearchField(facetFieldName, SearchFieldDataType.STRING),
          facetValues
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
