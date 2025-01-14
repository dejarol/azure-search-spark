package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.models.SearchOptions
import com.github.jarol.azure.search.spark.sql.connector.read.SearchOptionsBuilder

/**
 * Parent class for Scala-based [[SearchPartition]](s)
 * @param partitionId partition id
 * @param optionsBuilder delegate object for building the search options for this partition
 */

abstract class AbstractSearchPartition(
                                        protected val partitionId: Int,
                                        protected val optionsBuilder: SearchOptionsBuilder
                                      )
  extends SearchPartition {

  override final def getPartitionId: Int = partitionId

  override final def getSearchOptions: SearchOptions = {

    partitionFilter
      .map(optionsBuilder.withFilter)
      .getOrElse(optionsBuilder)
      .buildOptions()
  }

  /**
   * Get the partition-specific filter to be applied by this instance
   * @return the partition filter for this instance
   */

  protected[partitioning] def partitionFilter: Option[String]
}
