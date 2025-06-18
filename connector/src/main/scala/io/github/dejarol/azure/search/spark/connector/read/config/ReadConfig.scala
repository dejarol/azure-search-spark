package io.github.dejarol.azure.search.spark.connector.read.config

import com.azure.search.documents.models.{FacetResult, SearchResult}
import com.azure.search.documents.util.SearchPagedIterable
import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.core.config.{ConfigException, ExtendableConfig, SearchConfig, SearchIOConfig}
import io.github.dejarol.azure.search.spark.connector.core.utils.SearchClients
import io.github.dejarol.azure.search.spark.connector.read.filter.{ODataExpression, ODataExpressions}
import io.github.dejarol.azure.search.spark.connector.read.partitioning._
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types.StructType

import java.util.{Iterator => Jiterator}
import scala.util.Try

/**
 * Read configuration
 * @param options read options passed to the datasource
 */

case class ReadConfig(override protected val options: CaseInsensitiveMap[String])
  extends SearchIOConfig(options)
    with ExtendableConfig[ReadConfig] {

  /**
   * Updates this configuration by upserting the given key-value pair
   * @param key key
   * @param value value
   * @return this configuration, with either a newly added key-value pair or an updated pair
   */

  override def withOption(key: String, value: String): ReadConfig = {

    this.copy(
      options = options + (key, value)
    )
  }

  /**
   * Extends this configuration by injecting an OData <code>\$filter</code>expression obtained by logically combining the predicates
   * that can be pushed down to the datasource
   * @param predicates predicates that can be pushed down
   * @return this configuration with new key-value pair related to the pushed predicate
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
   * Extends this configuration by adding a select clause based on the provided schema.
   * If the schema is not empty, it creates a comma-separated list of field names
   * and adds it as a SELECT option to the search configuration.
   * @param schema schema of the data to be selected. If empty, no SELECT clause will be added.
   * @return this config instance, maybe with a select clause
   */

  def withSelectClause(schema: StructType): ReadConfig = {

    if (schema.isEmpty) {
      this
    } else {
      withOption(
        ReadConfig.SEARCH_OPTIONS_PREFIX + SearchOptionsBuilderImpl.SELECT,
        schema.map(_.name).mkString(",")
      )
    }
  }

  /**
   * Collect options related to documents search
   * @return a [[SearchOptionsBuilderImpl]] instance
   */

  def searchOptionsBuilderConfig: SearchOptionsBuilderImpl = {

    SearchOptionsBuilderImpl(
      getAllWithPrefix(ReadConfig.SEARCH_OPTIONS_PREFIX)
    )
  }

  /**
   * Gets the user-defined [[io.github.dejarol.azure.search.spark.connector.read.partitioning.PartitionerFactory]]
   * (for creating the partitioner responsible for planning input partitions).
   * <br>
   * If not provided, the default implementation (
   * [[io.github.dejarol.azure.search.spark.connector.read.partitioning.DefaultPartitioner]])
   * will be used
   * @throws io.github.dejarol.azure.search.spark.connector.core.config.ConfigException
   * if the partitioner type is not valid or any of the partitioner options is missing/invalid
   * @return a partitioner factory instance
   */

  @throws[ConfigException]
  def partitionerFactory: PartitionerFactory = {

    get(ReadConfig.PARTITIONER_CLASS_CONFIG) match {
      case Some(value) => value.toLowerCase match {
        case ReadConfig.RANGE_PARTITIONER_CLASS_VALUE => new RangePartitionerFactory
        case ReadConfig.FACETED_PARTITIONER_CLASS_VALUE => new FacetedPartitionerFactory
        case _ => createCustomPartitionerFactory(value)
      }

      // If not defined, provide an anonymous factory that returns the default partitioner
      case None => (_: ReadConfig) => DefaultPartitioner()
    }
  }

  /**
   * Creates a custom partitioner factory by using the provided class name. This value should be the
   * fully qualified name of a class that extends [[io.github.dejarol.azure.search.spark.connector.read.partitioning.PartitionerFactory]]
   * and provides a single no-arg constructor
   * @param factoryClassName fully qualified name of the factory class
   * @throws ConfigException if factory creation fails
   * @return a partitioner factory instance
   */

  @throws[ConfigException]
  private def createCustomPartitionerFactory(factoryClassName: String): PartitionerFactory = {

    Try {
      Class.forName(factoryClassName)
        .getDeclaredConstructor()
        .newInstance()
        .asInstanceOf[PartitionerFactory]
    }.toEither match {
      case Left(cause) => throw ConfigException.forIllegalOptionValue(
        ReadConfig.PARTITIONER_CLASS_CONFIG,
        factoryClassName,
        s"The only valid values are '${ReadConfig.RANGE_PARTITIONER_CLASS_VALUE}', " +
          s"'${ReadConfig.FACETED_PARTITIONER_CLASS_VALUE}' or the fully qualified name of a custom class " +
          s"that extends ${classOf[PartitionerFactory].getName} and provides a single no-arg constructor",
        cause
      )
      case Right(value) => value
    }
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

  /**
   * Get the result obtained by querying documents combining inner Search options with the filter
   * defined by a given [[io.github.dejarol.azure.search.spark.connector.read.partitioning.SearchPartition]]
   * @param partition a Search partition
   * @param includeTotalCount whether to include the <code>totalCount</code> property in the result
   * @return an object representing the Search result
   */

  private def getSearchPagedIterable(
                                      partition: SearchPartition,
                                      includeTotalCount: Boolean
                                    ): SearchPagedIterable = {

    // Enrich the original builder with the partition filter, if defined
    val originalBuilder = searchOptionsBuilderConfig
    val enrichedOptions = Option(partition.getPartitionFilter)
      .map(originalBuilder.addFilter)
      .getOrElse(originalBuilder)

    // Retrieve the results
    withSearchClientDo {
      client =>
        SearchClients.getSearchPagedIterable(
          client,
          enrichedOptions.searchText.orNull,
          enrichedOptions.buildOptions().setIncludeTotalCount(includeTotalCount)
        )
    }
  }

  /**
   * Get the overall Search result by querying documents combining inner Search options with the
   * filter defined by a partition, and get an iterator with retrieved results
   * @param partition a Search partition
   * @return an iterator of [[com.azure.search.documents.models.SearchResult]]
   */

  def getResultsForPartition(partition: SearchPartition): Jiterator[SearchResult] = {

    getSearchPagedIterable(
      partition,
      includeTotalCount = false
    ).iterator()
  }

  /**
   * Get the (estimated) number of documents for a partition
   * @param partition a Search partition
   * @return the (estimated) count of documents retrieved by the given partition
   */

  def getCountForPartition(partition: SearchPartition): Long = {

    getSearchPagedIterable(
      partition,
      includeTotalCount = true
    ).getTotalCount
  }

  /**
   * Get the [[com.azure.search.documents.models.FacetResult]](s) from a facetable field
   * @param facetField name of the facetable field
   * @param facetExpression facet expression
   * @return a collection of facet results
   */

  def getFacets(
                 facetField: String,
                 facetExpression: String
               ): Seq[FacetResult] = {

    // Get the builder, add the facet
    val builder = searchOptionsBuilderConfig.addFacet(facetExpression)
    val listOfFacetResult = withSearchClientDo {
      client =>
        SearchClients.getSearchPagedIterable(
          client,
          builder.searchText.orNull,
          builder.buildOptions()
      )
    }.getFacets.get(facetField)

    // Convert the java List to a Seq
    JavaScalaConverters.listToSeq(listOfFacetResult)
  }
}

object ReadConfig {

  final val PARTITIONER_CLASS_CONFIG = "partitioner"
  final val RANGE_PARTITIONER_CLASS_VALUE = "range"
  final val FACETED_PARTITIONER_CLASS_VALUE = "faceted"
  final val PUSHDOWN_PREDICATE_CONFIG = "pushdownPredicate"
  final val SEARCH_OPTIONS_PREFIX = "searchOptions."
  final val PARTITIONER_OPTIONS_PREFIX = "partitioner.options."

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
