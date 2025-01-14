package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.models.SearchOptions
import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.read.SearchOptionsBuilder

class DefaultSearchPartitionSpec
  extends BasicSpec {

  /**
   * Create a partition instance
   * @param optionsBuilder a Search options supplier
   * @return a partition instance
   */

  private def createPartition(optionsBuilder: SearchOptionsBuilder): DefaultSearchPartition = {

    DefaultSearchPartition(0, optionsBuilder)
  }

  describe(anInstanceOf[DefaultSearchPartition]) {
    describe(SHOULD_NOT) {
      it("define any partition filter") {

        val emptySupplier = new SearchOptionsBuilder {
          override def buildOptions(): SearchOptions = new SearchOptions
          override def withFilter(other: String): SearchOptionsBuilder = this
        }

        createPartition(emptySupplier).partitionFilter shouldBe empty
      }
    }
  }
}
