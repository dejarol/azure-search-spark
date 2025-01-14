package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.read.SearchOptionsSupplier

/**
 * Default implementation
 * @param partitionId partition id
 * @param optionsSupplier delegate object for getting the search options for this partition
 */

case class DefaultSearchPartition(
                                  override protected val partitionId: Int,
                                  override protected val optionsSupplier: SearchOptionsSupplier
                                )
  extends AbstractSearchPartition(partitionId, optionsSupplier) {

  override protected[partitioning] def partitionFilter: Option[String] = None
}
