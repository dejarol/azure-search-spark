package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}

class AbstractFacetPartitionSpec
  extends BasicSpec
    with FieldFactory {

  private lazy val defaultFacetFilter = "field eq value"

  /**
   * Create a partition instance
   * @param inputFilter input filter
   * @return a partition instance
   */

  private def createPartition(inputFilter: Option[String]): AbstractFacetPartition = {

    new AbstractFacetPartition(
      0,
      inputFilter,
      None,
      "facetFieldName"
    ) {

      override def facetFilter: String = defaultFacetFilter
    }
  }

  describe(anInstanceOf[AbstractFacetPartition]) {
    describe(SHOULD) {
      describe("create a partition filter") {
        it("combining facet filter and input filter") {

          // Non-empty input filter: the partition filter should combine the two
          val inputFilter = "inputFilter"
          createPartition(
            Some(inputFilter)
          ).getSearchFilter shouldBe s"$inputFilter and $defaultFacetFilter"

          // Empty input filter: partition filter should consist only of facet filter
          createPartition(None).getSearchFilter shouldBe defaultFacetFilter
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
