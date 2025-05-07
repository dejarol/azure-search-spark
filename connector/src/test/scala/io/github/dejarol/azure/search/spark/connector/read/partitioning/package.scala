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

  def stubPartitioner(partitions: Seq[SearchPartition]): SearchPartitioner = {

    val partitioner = stub[SearchPartitioner]
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

  def stubPartitionById(id: Int): SearchPartition = {

    val partition = stub[SearchPartition]
    (partition.getPartitionId _).when().returns(id)
    partition
  }

  /**
   * Creates a mock partition, that will return the provided filter
   * @param filter partition filter
   * @return a mock partition
   */

  def stubPartitionByFilter(filter: String): SearchPartition = {

    val partition = stub[SearchPartition]
    (partition.getPartitionFilter _).when().returns(filter)
    partition
  }
}
