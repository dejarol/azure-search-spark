package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

/**
 * Default implementation
 * @param partitionId partition id
 */

case class DefaultSearchPartition(override protected val partitionId: Int)
  extends AbstractSearchPartition(partitionId) {

  override def getPartitionFilter: String = null
}
