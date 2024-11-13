package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.SearchSparkITSpec

class FacetNullValuePartitionITSpec
  extends SearchSparkITSpec {

  private lazy val FACET_FIELD_IS_NULL = "facet field is null"
  private lazy val NOT_MATCHING_OTHER_VALUES = "does not match other facet values"

  describe(anInstanceOf[FacetNullValuePartition]) {
    describe(SHOULD) {
      describe("retrieve documents") {
        it(s"whose $FACET_FIELD_IS_NULL or $NOT_MATCHING_OTHER_VALUES") {

          // TODO
        }

        it(s"that match a filter and its $FACET_FIELD_IS_NULL or $NOT_MATCHING_OTHER_VALUES") {

          // TODO
        }
      }
    }
  }
}
