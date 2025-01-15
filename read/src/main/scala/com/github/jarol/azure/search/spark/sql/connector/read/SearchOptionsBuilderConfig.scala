package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.models.{QueryType, SearchOptions}
import com.github.jarol.azure.search.spark.sql.connector.core.config.SearchConfig
import com.github.jarol.azure.search.spark.sql.connector.core.utils.Enums
import com.github.jarol.azure.search.spark.sql.connector.read.SearchOptionsOperations._
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * Collection of options related to [[SearchOptions]] building
 * @param options case-insensitive options
 */

case class SearchOptionsBuilderConfig(override protected val options: CaseInsensitiveMap[String])
  extends SearchConfig(options)
    with SearchOptionsBuilder {

  /**
   * Return the text to be searched when querying documents
   * @return the text to be searched when querying documents
   */

  private[read] def searchText: Option[String] = get(SearchOptionsBuilderConfig.SEARCH)

  /**
   * Return the set of index fields to select. If not provided, all retrievable fields will be selected
   * @return index fields to select
   */

  private[read] def select: Option[Seq[String]] = getAsList(SearchOptionsBuilderConfig.SELECT_CONFIG)

  /**
   * Get the filter to apply on index documents. The filter must follow OData syntax
   * @return the filter to apply on search index documents
   */

  private[read] def filter: Option[String] = get(SearchOptionsBuilderConfig.FILTER)

  /**
   * Get the query type to use for querying documents
   * @return query type
   */

  private[read] def queryType: Option[QueryType] = {

    getAs(
      SearchOptionsBuilderConfig.QUERY_TYPE,
      name => Enums.unsafeValueOf[QueryType](
        name,
        (queryType, value) => queryType.name().equalsIgnoreCase(value)
      )
    )
  }

  /**
   * Get the facets to set when querying document
   * @return facets to use for querying documents
   */

  private[read] def facets: Option[Seq[String]] = getAsList(SearchOptionsBuilderConfig.FACETS, '|')

  /**
   * Create a new config instance by upserting the underlying options (i.e., by either adding a new
   * key-value pair if such key does not exist or by replacing its value otherwise)
   * @param key key
   * @param value value
   * @return a new config instance
   */

  private[read] def withOption(key: String, value: String): SearchOptionsBuilderConfig = {

    this.copy(options = options + (key, value))
  }

  override def withFilter(other: String): SearchOptionsBuilderConfig = {

    val newFilterValue: String = filter.map {
      old => s"($old) and ($other)"
    }.getOrElse(other)

    withOption(SearchOptionsBuilderConfig.FILTER, newFilterValue)
  }

  override def withFacet(facet: String): SearchOptionsBuilderConfig = {

    val newFacetsValue: String = facets.map(_ :+ facet)
      .getOrElse(Seq(facet))
      .mkString("|")

    withOption(
      SearchOptionsBuilderConfig.FACETS,
      newFacetsValue
    )
  }

  override def buildOptions(): SearchOptions = {

    new SearchOptions()
      .setFilter(filter)
      .setSelect(select)
      .setFacets(facets)
      .setQueryType(queryType)
  }
}

object SearchOptionsBuilderConfig {

  final val SEARCH = "search"
  final val FILTER = "filter"
  final val SELECT_CONFIG = "select"
  final val QUERY_TYPE = "queryType"
  final val FACETS = "facets"

  /**
   * Create an instance from a [[SearchConfig]]
   * @param config config instance
   * @return an instance of [[SearchOptionsBuilderConfig]]
   */

  def apply(config: SearchConfig): SearchOptionsBuilderConfig = {

    SearchOptionsBuilderConfig(
      CaseInsensitiveMap(config.toMap)
    )
  }
}
