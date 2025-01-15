package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}
import com.github.jarol.azure.search.spark.sql.connector.read.SearchOptionsBuilder
import org.scalamock.scalatest.MockFactory

class AbstractFacetPartitionSpec
  extends BasicSpec
    with FieldFactory
      with MockFactory {

  private lazy val defaultFacetFilter = "field eq value"
  private lazy val mockBuilder = mock[SearchOptionsBuilder]

  /**
   * Create a partition instance
   * @return a partition instance
   */

  private def createPartition(): AbstractFacetPartition = {

    new AbstractFacetPartition(
      0,
      mockBuilder,
      "facetFieldName"
    ) {

      override protected[partitioning] def facetFilter: String = defaultFacetFilter
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
          mockBuilder,
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
