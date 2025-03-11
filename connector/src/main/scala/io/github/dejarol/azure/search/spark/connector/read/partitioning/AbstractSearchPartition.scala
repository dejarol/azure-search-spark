package io.github.dejarol.azure.search.spark.connector.read.partitioning

/**
 * Parent class for Scala-based [[SearchPartition]](s)
 * @param partitionId partition id
 */

abstract class AbstractSearchPartition(protected val partitionId: Int)
  extends SearchPartition {

  override final def getPartitionId: Int = partitionId
}
