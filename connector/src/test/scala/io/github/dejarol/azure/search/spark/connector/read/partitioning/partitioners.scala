package io.github.dejarol.azure.search.spark.connector.read.partitioning

import io.github.dejarol.azure.search.spark.connector.read.config.ReadConfig

/**
 * Empty partitioner factory defined for testing purposes
 */

class EmptyScalaPartitionerFactory
  extends PartitionerFactory {

  override def createPartitioner(readConfig: ReadConfig): SearchPartitioner = {

    stubPartitioner(
      Seq.empty
    )
  }
}

/**
 * A simple partitioner factory defined for testing purposes.
 * When invoked, it should throw an exception as it does not provide any no-arguments constructor
 * @param partitionId partition id (to be used by the mock partitions)
 */

class SinglePartitionFactory(private val partitionId: Int)
  extends PartitionerFactory {

  override def createPartitioner(readConfig: ReadConfig): SearchPartitioner = {

    stubPartitioner(
      Seq(
        stubPartitionById(partitionId)
      )
    )
  }
}
