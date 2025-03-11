package io.github.dejarol.azure.search.spark.connector.read.partitioning

import com.azure.search.documents.indexes.models.SearchFieldDataType
import io.github.dejarol.azure.search.spark.connector.{BasicSpec, FieldFactory}

class AbstractFacetPartitionSpec
  extends BasicSpec
    with FieldFactory {

  describe(`object`[AbstractFacetPartition]) {
    describe(SHOULD) {
      it("create a collection of partitions") {

        val facetFieldName = "field"
        val facetValues = Seq("v1", "v2", "v3")
        val actual = AbstractFacetPartition.createCollection(
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
