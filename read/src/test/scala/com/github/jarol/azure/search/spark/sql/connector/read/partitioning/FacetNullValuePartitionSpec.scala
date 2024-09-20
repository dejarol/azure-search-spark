package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import org.scalatest.Inspectors

class FacetNullValuePartitionSpec
  extends BasicSpec
    with Inspectors {

  private lazy val facetFieldName = "field"
  private lazy val facetValues = Seq("v1", "v2")

  describe(anInstanceOf[FacetNullValuePartition]) {
    describe(SHOULD) {
      it("have partitionId equal to the size of facet values") {

        FacetNullValuePartition(
          None,
          None,
          facetFieldName,
          facetValues
        ).getPartitionId shouldBe facetValues.size
      }

      it("create a facet filter that includes null or different values") {

        val actual: String = FacetNullValuePartition(
          None,
          None,
          facetFieldName,
          facetValues
        ).facetFilter

        val eqNull = s"$facetFieldName eq null"
        val equalToOtherValues = facetValues.map {
          value => s"$facetFieldName eq $value"
        }.mkString(" or ")

        val expected  = s"$eqNull or not ($equalToOtherValues)"
        actual.contains(eqNull) shouldBe true
        actual.contains(equalToOtherValues) shouldBe true
        actual shouldBe expected
      }
    }
  }
}
