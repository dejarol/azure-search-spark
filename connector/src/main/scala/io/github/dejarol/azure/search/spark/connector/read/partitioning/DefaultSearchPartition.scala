package io.github.dejarol.azure.search.spark.connector.read.partitioning

/**
 * Default partition implementation (no filter defined)
 * @param partitionId partition id
 */

case class DefaultSearchPartition(override protected val partitionId: Int)
  extends AbstractSearchPartition(partitionId) {

  override def getPartitionFilter: String = null
}
