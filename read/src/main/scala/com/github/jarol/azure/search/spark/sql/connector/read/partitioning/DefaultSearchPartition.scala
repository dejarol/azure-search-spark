package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.read.SearchOptionsBuilder

/**
 * Default implementation
 * @param partitionId partition id
 * @param optionsBuilder delegate object for building the search options for this partition
 */

case class DefaultSearchPartition(
                                  override protected val partitionId: Int,
                                  override protected val optionsBuilder: SearchOptionsBuilder
                                )
  extends AbstractSearchPartition(partitionId, optionsBuilder) {

  override protected[partitioning] def partitionFilter: Option[String] = None
}
