package io.github.dejarol.azure.search.spark.connector.read.partitioning

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import io.github.dejarol.azure.search.spark.connector.core.schema.SearchFieldFeature
import io.github.dejarol.azure.search.spark.connector.{BasicSpec, FieldFactory}
import org.scalatest.EitherValues

class FacetedPartitionerFactorySpec
  extends BasicSpec
    with FieldFactory
      with EitherValues {

  private lazy val first = "first"

  /**
   * Enable features on a [[SearchField]]
   * @param field    field
   * @param features features to enable
   */

  private def enableFeatures(
                              field: SearchField,
                              features: SearchFieldFeature*
                            ): SearchField = {

    features.foldLeft(field) {
      case (field, feature) =>
        feature.enableOnField(field)
    }
  }

  describe(`object`[FacetedPartitionerFactory.type ]) {
    describe(SHOULD) {
      describe("evaluate the number of partitions returning") {
        describe("a Right when") {
          it("no number is provided") {

            FacetedPartitionerFactory.evaluatePartitionNumber(
              None
            ) shouldBe 'right
          }

          it("a valid number is provided") {

            FacetedPartitionerFactory.evaluatePartitionNumber(
              Some(2)
            ) shouldBe 'right
          }
        }

        describe("a Left for") {
          it("invalid numbers") {

            FacetedPartitionerFactory.evaluatePartitionNumber(
              Some(1)
            ) shouldBe 'left
          }
        }
      }

      describe("evaluate if a Search field is eligible for faceting") {
        describe("returning a Right for") {
          it("an existing facetable and filterable field") {

            FacetedPartitionerFactory.getCandidateFacetField(
              first,
              Seq(
                enableFeatures(
                  createSearchField(first, SearchFieldDataType.STRING),
                  SearchFieldFeature.FACETABLE,
                  SearchFieldFeature.FILTERABLE
                )
              )
            ) shouldBe 'right
          }
        }

        describe("a Left for") {
          it("non-existing field") {

            FacetedPartitionerFactory.getCandidateFacetField(
              first,
              Seq.empty
            ) shouldBe 'left
          }

          it("a non-filterable or facetable field") {

            FacetedPartitionerFactory.getCandidateFacetField(
              first,
              Seq(
                enableFeatures(
                  createSearchField(first, SearchFieldDataType.STRING),
                  SearchFieldFeature.FACETABLE
                )
              )
            ) shouldBe 'left

            FacetedPartitionerFactory.getCandidateFacetField(
              first,
              Seq(
                enableFeatures(
                  createSearchField(first, SearchFieldDataType.STRING),
                  SearchFieldFeature.FILTERABLE
                )
              )
            ) shouldBe 'left
          }
        }
      }
    }
  }
}
