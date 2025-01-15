package com.github.jarol.azure.search.spark.sql.connector.read.config

import com.azure.search.documents.models.SearchOptions
import com.azure.search.documents.util.SearchPagedIterable
import com.github.jarol.azure.search.spark.sql.connector.core.config.{ExtendableConfig, SearchConfig, SearchIOConfig}
import com.github.jarol.azure.search.spark.sql.connector.core.utils.SearchUtils
import com.github.jarol.azure.search.spark.sql.connector.read.filter.{ODataExpression, ODataExpressions}
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.{DefaultPartitioner, SearchPartitioner}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * Read configuration
 * @param options read options passed to the datasource
 */

case class ReadConfig(override protected val options: CaseInsensitiveMap[String])
  extends SearchIOConfig(options)
    with ExtendableConfig[ReadConfig] {

  override def withOption(key: String, value: String): ReadConfig = {

    this.copy(
      options = options + (key, value)
    )
  }

  /**
   * Extend this configuration by injecting an OData expression obtained by logically combining the predicates
   * that can be pushed down to the datasource
   * @param predicates predicates that can be pushed down
   * @return a new configuration instance
   */

  def withPushedPredicates(predicates: Seq[ODataExpression]): ReadConfig = {

    // If there are no predicates, do not set the option
    if (predicates.isEmpty) {
      this
    } else {

      // The pushed predicate is either the first predicate or the logical AND combination of all predicates
      val pushedPredicate = if (predicates.size.equals(1)) {
        predicates.head.toUriLiteral
      } else {
        ODataExpressions.logical(predicates, isAnd = true).toUriLiteral
      }

      withOption(
        ReadConfig.SEARCH_OPTIONS_PREFIX + SearchOptionsBuilderImpl.PUSHED_PREDICATE,
        pushedPredicate
      )
    }
  }

  /**
   * Collect options related to documents search
   *
   * @return a [[SearchOptionsBuilderImpl]] instance
   */

  def searchOptionsBuilderConfig: SearchOptionsBuilderImpl = {

    SearchOptionsBuilderImpl(
      getAllWithPrefix(ReadConfig.SEARCH_OPTIONS_PREFIX)
    )
  }

  /**
   * Execute a Search on target index
   * @param searchOptions search options
   * @return an iterable of Search results
   */

  def search(searchOptions: SearchOptions): SearchPagedIterable = {

    withSearchClientDo {
      sc => SearchUtils.getSearchPagedIterable(sc, searchOptions)
    }
  }

  /**
   * Get the [[SearchPartitioner]] to use for generating the search partitions.
   * If not provided, a [[DefaultPartitioner]] will be used
   *
   * @return a search partitioner instance
   */

  def partitionerClass: Class[SearchPartitioner] = {

    getOrDefaultAs[Class[SearchPartitioner]](
      ReadConfig.PARTITIONER_CLASS_CONFIG,
      classOf[DefaultPartitioner].asInstanceOf[Class[SearchPartitioner]],
      s => Class.forName(s).asInstanceOf[Class[SearchPartitioner]]
    )
  }

  /**
   * Retrieve the options related to specified partitioner
   * @return the partitioner options
   */

  def partitionerOptions: SearchConfig = getAllWithPrefix(ReadConfig.PARTITIONER_OPTIONS_PREFIX)

  /**
   * Return the flag that indicates if predicate pushdown should be enabled when querying data
   * @return true for enabling predicate pushdown
   */

  def pushdownPredicate: Boolean = getOrDefaultAs[Boolean](ReadConfig.PUSHDOWN_PREDICATE_CONFIG, true, _.toBoolean)
}

object ReadConfig {

  final val PARTITIONER_CLASS_CONFIG = "partitioner"
  final val PUSHDOWN_PREDICATE_CONFIG = "pushDownPredicate"
  final val SEARCH_OPTIONS_PREFIX = "searchOptions."
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
