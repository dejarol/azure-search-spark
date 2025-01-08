package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec

class FacetNullValuePartitionSpec
  extends BasicSpec {

  private lazy val facetFieldName = "field"
  private lazy val facetValues = Seq("v1", "v2")
  private lazy val partition = FacetNullValuePartition(
    None,
    None,
    Seq.empty,
    facetFieldName,
    facetValues
  )

  describe(anInstanceOf[FacetNullValuePartition]) {
    describe(SHOULD) {
      it("have partitionId equal to the size of facet values") {

        partition.getPartitionId shouldBe facetValues.size
      }

      it("create a facet filter that includes null or different values") {

        val actual = partition.facetFilter
        val eqNull = s"$facetFieldName eq null"
        val equalToOtherValues = facetValues.map {
          value => s"$facetFieldName eq $value"
        }.mkString(" or ")

        val expected  = s"$eqNull or not ($equalToOtherValues)"
        actual.contains(eqNull) shouldBe true
        actual.contains(equalToOtherValues) shouldBe true
        actual shouldBe expected
      }

      // TODO: add test for combining odata expr
    }
  }
}
