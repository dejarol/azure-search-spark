package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.SearchFieldFeature
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}
import org.scalatest.EitherValues

class RangePartitionerSpec
  extends BasicSpec
    with FieldFactory
      with EitherValues {

  private lazy val (first, second, third, fourth) = ("first", "second", "third", "fourth")
  private lazy val validTypes = Seq(
    SearchFieldDataType.INT32,
    SearchFieldDataType.INT64,
    SearchFieldDataType.DOUBLE,
    SearchFieldDataType.DATE_TIME_OFFSET
  )

  describe(`object`[RangePartitioner]) {
    describe(SHOULD) {
      describe("evaluate if an existing field") {
        describe("is candidate for partitioning returning") {
          it("a Right for valid cases") {

            forAll(validTypes) {
              tp =>

                val field = SearchFieldFeature.FILTERABLE.enableOnField(
                  createSearchField("first", tp)
                )

                field shouldBe enabledFor(SearchFieldFeature.FILTERABLE)
                RangePartitioner.evaluateExistingCandidate(field) shouldBe 'right
            }
          }

          describe("a Left for") {
            it("non filterable fields") {

              forAll(validTypes) {
                tp =>

                  val field = createSearchField("first", tp)
                  field should not be enabledFor(SearchFieldFeature.FILTERABLE)
                  RangePartitioner.evaluateExistingCandidate(field) shouldBe 'left
              }
            }

            it("non-numeric or date time fields") {

              forAll(
                Seq(
                  SearchFieldDataType.SINGLE,
                  SearchFieldDataType.STRING,
                  SearchFieldDataType.COMPLEX
                )
              ) {
                tp =>
                  val field = SearchFieldFeature.FILTERABLE.enableOnField(
                    createSearchField("first", tp)
                  )

                  field shouldBe enabledFor(SearchFieldFeature.FILTERABLE)
                  RangePartitioner.evaluateExistingCandidate(field) shouldBe 'left
              }
            }
          }
        }
      }

      describe("safely retrieve a partition field") {

        val fields = Seq(
          SearchFieldFeature.FILTERABLE.enableOnField(createSearchField(first, SearchFieldDataType.INT32)),
          SearchFieldFeature.FILTERABLE.enableOnField(createSearchField(second, SearchFieldDataType.STRING)),
          createSearchField(third, SearchFieldDataType.DATE_TIME_OFFSET)
        )

        describe("returning a Right for") {

          it("existing, filterable and type-wise valid fields") {

            RangePartitioner.getPartitionField(
              fields,
              first
            ) shouldBe 'right
          }
        }

        describe("returning a Left for") {
          it("non existing fields") {

            RangePartitioner.getPartitionField(
              fields,
              fourth
            ) shouldBe 'left

          }

          it("non filterable fields") {

            RangePartitioner.getPartitionField(
              fields,
              third
            ) shouldBe 'left
          }

          it("type-wise illegal fields") {

            RangePartitioner.getPartitionField(
              fields,
              second
            ) shouldBe 'left
          }
        }
      }
    }
  }
}
