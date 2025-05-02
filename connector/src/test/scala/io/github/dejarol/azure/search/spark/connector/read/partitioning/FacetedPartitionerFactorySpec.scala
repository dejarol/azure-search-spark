package io.github.dejarol.azure.search.spark.connector.read.partitioning

import io.github.dejarol.azure.search.spark.connector.{BasicSpec, FieldFactory}
import org.scalatest.EitherValues

class FacetedPartitionerFactorySpec
  extends BasicSpec
    with FieldFactory
      with EitherValues {

  describe(`object`[FacetedPartitionerFactory.type ]) {
    describe(SHOULD) {
      describe("evaluate if an existing field") {
        describe("is candidate for faceting returning") {
          it("a Right for valid cases") {

            // TODO: test
          }

          describe("a Left for") {
            it("non-filterable fields") {

              // TODO: test
            }

            it("non-facetable fields") {

              // TODO: test
            }

            it("non suitable dtypes") {

              // TODO: test
            }
          }
        }
      }
    }
  }
}
