package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.models.{QueryType, SearchOptions}
import com.github.jarol.azure.search.spark.sql.connector.core.config.SearchConfig
import com.github.jarol.azure.search.spark.sql.connector.core.utils.Enums
import com.github.jarol.azure.search.spark.sql.connector.read.SearchOptionsOperations._
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * Collection of user-defined options related to data reading
 * @param options case-insensitive options
 */

case class SearchOptionsConfig(override protected val options: CaseInsensitiveMap[String])
  extends SearchConfig(options)
    with SearchOptionsSupplier {

  /**
   * Return the set of index fields to select. If not provided, all retrievable fields will be selected
   * @return index fields to select
   */

  private[read] def select: Option[Seq[String]] = getAsList(SearchOptionsConfig.SELECT_CONFIG)

  /**
   * Get the filter to apply on index documents. The filter must follow OData syntax
   * @return the filter to apply on search index documents
   */

  private[read] def filter: Option[String] = get(SearchOptionsConfig.FILTER_CONFIG)

  /**
   * Get the query type to use for querying documents
   * @return query type
   */

  private[read] def queryType: Option[QueryType] = {

    getAs(
      SearchOptionsConfig.QUERY_TYPE,
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

  private[read] def facets: Option[Seq[String]] = getAsList(SearchOptionsConfig.FACETS, '|')

  protected[read] def withOption(key: String, value: String): SearchOptionsConfig = {

    this.copy(
      options = options + (key, value)
    )
  }

  def withFacets(facet: String, facets: String*): SearchOptionsConfig = {

    val allFacets: Seq[String] = facet +: facets
    withOption(
      SearchOptionsConfig.FACETS,
      allFacets.mkString("|")
    )
  }

  override def createSearchOptions(): SearchOptions = {

    new SearchOptions()
      .setFilter(filter)
      .setSelect(select)
      .setFacets(facets)
      .setQueryType(queryType)
  }
}

object SearchOptionsConfig {

  final val FILTER_CONFIG = "filter"
  final val SELECT_CONFIG = "select"
  final val QUERY_TYPE = "queryType"
  final val FACETS = "facets"

  /**
   * Create an instance from a [[SearchConfig]]
   * @param config config instance
   * @return an instance of [[SearchOptionsConfig]]
   */

  def apply(config: SearchConfig): SearchOptionsConfig = {

    SearchOptionsConfig(
      CaseInsensitiveMap(config.toMap)
    )
  }
}
