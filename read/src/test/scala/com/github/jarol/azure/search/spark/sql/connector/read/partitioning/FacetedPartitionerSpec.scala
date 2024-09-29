package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.{SearchFieldFeature, toSearchFieldOperations}
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}
import org.scalatest.EitherValues

class FacetedPartitionerSpec
  extends BasicSpec
    with FieldFactory
      with EitherValues {

  private lazy val first = "first"

  describe(`object`[FacetedPartitioner]) {
    describe(SHOULD) {
      describe("evaluate the number of partitions returning") {
        describe("a Right when") {
          it("no number is provided") {

            FacetedPartitioner.evaluatePartitionNumber(
              None
            ) shouldBe 'right
          }

          it("a valid number is provided") {

            FacetedPartitioner.evaluatePartitionNumber(
              Some(2)
            ) shouldBe 'right
          }
        }

        describe("a Left for") {
          it("invalid numbers") {

            FacetedPartitioner.evaluatePartitionNumber(
              Some(1)
            ) shouldBe 'left
          }
        }
      }

      describe("evaluate if a Search field is eligible for faceting") {
        describe("returning a Right for") {
          it("an existing facetable and filterable field") {

            FacetedPartitioner.getCandidateFacetField(
              first,
              Seq(
                createSearchField(first, SearchFieldDataType.STRING)
                  .enableFeatures(
                    SearchFieldFeature.FACETABLE,
                    SearchFieldFeature.FILTERABLE
                  )
              )
            ) shouldBe 'right
          }
        }

        describe("a Left for") {
          it("non-existing field") {

            FacetedPartitioner.getCandidateFacetField(
              first,
              Seq.empty
            ) shouldBe 'left
          }

          it("a non-filterable or facetable field") {

            FacetedPartitioner.getCandidateFacetField(
              first,
              Seq(
                createSearchField(first, SearchFieldDataType.STRING)
                  .enableFeatures(SearchFieldFeature.FACETABLE)
              )
            ) shouldBe 'left

            FacetedPartitioner.getCandidateFacetField(
              first,
              Seq(
                createSearchField(first, SearchFieldDataType.STRING)
                  .enableFeatures(SearchFieldFeature.FILTERABLE)
              )
            ) shouldBe 'left
          }
        }
      }
    }
  }
}
