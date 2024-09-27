package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.core.util.Context
import com.azure.search.documents.models.SearchOptions
import com.azure.search.documents.util.SearchPagedIterable
import com.github.jarol.azure.search.spark.sql.connector.core.config.{SearchConfig, SearchIOConfig, UsageMode}
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.{SearchPartitioner, SinglePartitionPartitioner}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * Read configuration
 * @param localOptions options passed to the [[org.apache.spark.sql.DataFrameReader]]
 * @param globalOptions read options retrieved from the underlying [[org.apache.spark.SparkConf]]
 */

case class ReadConfig(override protected val localOptions: CaseInsensitiveMap[String],
                      override protected val globalOptions: CaseInsensitiveMap[String])
  extends SearchIOConfig(localOptions, globalOptions) {

  /**
   * Execute a Search on target index
   * @param searchOptions search options
   * @return an iterable of Search results
   */

  final def search(searchOptions: SearchOptions): SearchPagedIterable = {

    withSearchClientDo {
      sc => sc.search(
        null,
        searchOptions,
        Context.NONE
      )
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

  def partitioner: SearchPartitioner = {

    getOrDefaultAs[SearchPartitioner](
      ReadConfig.PARTITIONER_CONFIG,
      SinglePartitionPartitioner(this),
      s => {
        ClassHelper.createInstance(
          Class.forName(s).asInstanceOf[Class[SearchPartitioner]],
          this
        )
      }
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

  def select: Option[Seq[String]] = getOptionalStringList(ReadConfig.SELECT_CONFIG)
}

object ReadConfig {

  final val FILTER_CONFIG = "filter"
  final val PARTITIONER_CONFIG = "partitioner"
  final val SELECT_CONFIG = "select"
  final val PARTITIONER_OPTIONS_PREFIX = "partitioner.options."
  final val FACET_FIELD_CONFIG = "facetField"
  final val NUM_PARTITIONS_CONFIG = "numPartitions"
  final val PARTITION_FIELD_CONFIG = "partitionField"
  final val LOWER_BOUND_CONFIG = "lowerBound"
  final val UPPER_BOUND_CONFIG = "upperBound"

  /**
   * Create an instance from both local and global options
   * @param locals local options
   * @param globals global options
   * @return a read config
   */

  def apply(locals: Map[String, String], globals: Map[String, String]): ReadConfig = {

    ReadConfig(
      CaseInsensitiveMap(locals),
      CaseInsensitiveMap(globals)
    )
  }

  /**
   * Create an instance from given options, retrieving SparkConf-related options
   * from the underlying active [[org.apache.spark.sql.SparkSession]] (if any)
   * @param options options passed via [[org.apache.spark.sql.DataFrameReader.option]] method
   * @return an instance of [[ReadConfig]]
   */

  def apply(options: Map[String, String]): ReadConfig = {

    ReadConfig(
      options,
      SearchIOConfig.allConfigsFromActiveSessionForMode(UsageMode.READ)
    )
  }
}
