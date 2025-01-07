package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.models.SearchOptions
import com.azure.search.documents.util.SearchPagedIterable
import com.github.jarol.azure.search.spark.sql.connector.core.config.{SearchConfig, SearchIOConfig}
import com.github.jarol.azure.search.spark.sql.connector.core.utils.SearchUtils
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.{SearchPartitioner, SinglePartitionPartitioner}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * Read configuration
 * @param options read options passed to the datasource
 */

case class ReadConfig(override protected val options: CaseInsensitiveMap[String])
  extends SearchIOConfig(options) {

  /**
   * Execute a Search on target index
   * @param searchOptions search options
   * @return an iterable of Search results
   */

  final def search(searchOptions: SearchOptions): SearchPagedIterable = {

    withSearchClientDo {
      sc => SearchUtils.getSearchPagedIterable(sc, searchOptions)
    }
  }

  /**
   * Get the filter to apply on index documents. The filter must follow OData syntax
   * ([[https://learn.microsoft.com/en-us/azure/search/search-query-odata-filter]])
   * @return the filter to apply on search index documents
   */

  def filter: Option[String] = get(ReadConfig.FILTER_CONFIG)

  /**
   * Get the [[SearchPartitioner]] to use for generating the search partitions.
   * If not provided, a [[SinglePartitionPartitioner]] will be used
   * @return a search partitioner instance
   */

  def partitionerClass: Class[SearchPartitioner] = {

    getOrDefaultAs[Class[SearchPartitioner]](
      ReadConfig.PARTITIONER_CLASS_CONFIG,
      classOf[SinglePartitionPartitioner].asInstanceOf[Class[SearchPartitioner]],
      s => Class.forName(s).asInstanceOf[Class[SearchPartitioner]]
    )
  }

  /**
   * Retrieve the options related to specified partitioner
   * @return the partitioner options
   */

  def partitionerOptions: SearchConfig = getAllWithPrefix(ReadConfig.PARTITIONER_OPTIONS_PREFIX)

  /**
   * Return the set of index fields to select. If not provided, all retrievable fields will be selected
   * @return index fields to select
   */

  def select: Option[Seq[String]] = getAsList(ReadConfig.SELECT_CONFIG)

  /**
   * Return the flag that indicates if predicate pushdown should be enabled when querying data
   * @return true for enabling predicate pushdown
   */

  def pushdownPredicate: Boolean = getOrDefaultAs[Boolean](ReadConfig.PUSHDOWN_PREDICATE_CONFIG, true, _.toBoolean)
}

object ReadConfig {

  final val FILTER_CONFIG = "filter"
  final val PARTITIONER_CLASS_CONFIG = "partitioner"
  final val SELECT_CONFIG = "select"
  final val PUSHDOWN_PREDICATE_CONFIG = "pushDownPredicate"
  final val PARTITIONER_OPTIONS_PREFIX = "partitioner.options."
  final val FACET_FIELD_CONFIG = "facetField"
  final val NUM_PARTITIONS_CONFIG = "numPartitions"
  final val PARTITION_FIELD_CONFIG = "partitionField"
  final val LOWER_BOUND_CONFIG = "lowerBound"
  final val UPPER_BOUND_CONFIG = "upperBound"

  /**
   * Create an instance from a simple map
   * @param dsOptions dataSource options
   * @return a read config
   */

  def apply(dsOptions: Map[String, String]): ReadConfig = {

    ReadConfig(
      CaseInsensitiveMap(dsOptions)
    )
  }
}
