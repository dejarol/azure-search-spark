package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.read.SearchOptionsBuilder
import org.scalamock.scalatest.MockFactory

class DefaultSearchPartitionSpec
  extends BasicSpec
    with MockFactory {

  private lazy val mockOptionsBuilder = mock[SearchOptionsBuilder]

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

        createPartition(mockOptionsBuilder).partitionFilter shouldBe empty
      }
    }
  }
}
