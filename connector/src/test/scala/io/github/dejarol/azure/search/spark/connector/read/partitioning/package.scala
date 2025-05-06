package io.github.dejarol.azure.search.spark.connector.read

import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import org.scalamock.scalatest.MockFactory

package object partitioning
  extends MockFactory {

  /**
   * Creates a mock partitioner, that will return the provided partitions
   * @param partitions partitions returned by the mock
   * @return a mock partitioner
   */

  def mockPartitioner(partitions: Seq[SearchPartition]): SearchPartitioner = {

    val partitioner = mock[SearchPartitioner]
    (partitioner.createPartitions _).when().returns(
      JavaScalaConverters.seqToList(
        partitions
      )
    )

    partitioner
  }

  /**
   * Creates a mock partition, that will return the provided id as partition id
   * @param id partition id
   * @return a mock partition
   */

  def mockPartition(id: Int): SearchPartition = {

    val partition = mock[SearchPartition]
    (partition.getPartitionId _).when().returns(id)
    partition
  }
}
