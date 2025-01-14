package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.models.SearchOptions
import com.azure.search.documents.util.SearchPagedIterable
import com.github.jarol.azure.search.spark.sql.connector.core.config.{SearchConfig, SearchIOConfig}
import com.github.jarol.azure.search.spark.sql.connector.core.utils.SearchUtils
import com.github.jarol.azure.search.spark.sql.connector.read.filter.ODataExpression
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.{SearchPartitioner, DefaultPartitioner}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * Read configuration
 * @param options read options passed to the datasource
 * @param pushedPredicates predicates to be pushed down
 */

case class ReadConfig(
                       override protected val options: CaseInsensitiveMap[String],
                       protected[read] val pushedPredicates: Seq[ODataExpression] = Seq.empty
                     )
  extends SearchIOConfig(options) {

  def withPushedPredicates(predicates: Seq[ODataExpression]): ReadConfig = this.copy(pushedPredicates = predicates)

  /**
   * Collect options related to documents search
 *
   * @return a [[SearchOptionsBuilderConfig]] instance
   */

  def searchOptionsBuilderConfig: SearchOptionsBuilderConfig = {

    SearchOptionsBuilderConfig(
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
      CaseInsensitiveMap(dsOptions),
      Seq.empty
    )
  }
}
