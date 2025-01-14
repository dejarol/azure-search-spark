package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.models.SearchOptions
import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.read.SearchOptionsSupplier

class DefaultSearchPartitionSpec
  extends BasicSpec {

  /**
   * Create a partition instance
   * @param optionsSupplier a Search options supplier
   * @return a partition instance
   */

  private def createPartition(optionsSupplier: SearchOptionsSupplier): DefaultSearchPartition = {

    DefaultSearchPartition(0, optionsSupplier)
  }

  describe(anInstanceOf[DefaultSearchPartition]) {
    describe(SHOULD_NOT) {
      it("define any partition filter") {

        val emptySupplier = new SearchOptionsSupplier {
          override def createSearchOptions(): SearchOptions = new SearchOptions
        }

        createPartition(emptySupplier).partitionFilter shouldBe empty
      }
    }
  }
}
