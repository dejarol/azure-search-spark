package com.github.jarol.azure.search.spark.sql.connector.read.config

import com.github.jarol.azure.search.spark.sql.connector.SearchITSpec
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SearchPartition
import org.scalamock.scalatest.MockFactory

class ReadConfigITSpec
  extends SearchITSpec
    with MockFactory {

  private lazy val partition = mock[SearchPartition]

  describe(anInstanceOf[ReadConfig]) {
    describe(SHOULD) {
      describe("retrieve") {
        it("an iterator of Search results for a partition") {

          // TODO: test
        }

        it("the count of documents for a partition") {

          // TODO: test
        }

        it("the facet results from a facetable field") {

          // TODO: test
        }
      }
    }
  }
}
