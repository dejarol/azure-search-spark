package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.models.SearchOptions
import com.github.jarol.azure.search.spark.sql.connector.read.SearchOptionsSupplier
import com.github.jarol.azure.search.spark.sql.connector.read.SearchOptionsOperations._

/**
 * Parent class for Scala-based [[SearchPartition]](s)
 * @param partitionId partition id
 * @param optionsSupplier delegate object for getting the search options for this partition
 */

abstract class AbstractSearchPartition(
                                        protected val partitionId: Int,
                                        protected val optionsSupplier: SearchOptionsSupplier
                                      )
  extends SearchPartition {

  override final def getPartitionId: Int = partitionId

  override final def getSearchOptions: SearchOptions = {

    // TODO: improve filter combination
    val originalOptions = optionsSupplier.createSearchOptions()
    val overAllFilter: Option[String] = AbstractSearchPartition.createODataFilter(
      Seq(partitionFilter, originalOptions.maybeFilter)
        .collect {
          case Some(value) => value
        }
    )

    originalOptions.setFilter(overAllFilter)
  }

  /**
   * Get the partition-specific filter to be applied by this instance
   * @return the partition filter for this instance
   */

  protected[partitioning] def partitionFilter: Option[String]
}

object AbstractSearchPartition {

  /**
   * Combine a collection of filters into a single OData filter
   * @param filters filters to combine
   * @return an overall filters that combine the input ones using <code>and</code>
   */

  private[partitioning] def createODataFilter(filters: Seq[String]): Option[String] = {

    if (filters.isEmpty) {
      None
    } else if (filters.size.equals(1)) {
      filters.headOption
    } else {
      Some(
        filters.map {
          filter => s"($filter)"
        }.mkString(" and ")
      )
    }
  }
}
