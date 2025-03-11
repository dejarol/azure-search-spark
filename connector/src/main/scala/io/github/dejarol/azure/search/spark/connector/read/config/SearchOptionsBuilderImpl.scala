package io.github.dejarol.azure.search.spark.connector.read.config

import com.azure.search.documents.models.{QueryType, SearchMode, SearchOptions}
import io.github.dejarol.azure.search.spark.connector.core.config.{ExtendableConfig, SearchConfig}
import io.github.dejarol.azure.search.spark.connector.core.utils.Enums
import io.github.dejarol.azure.search.spark.connector.read.SearchOptionsOperations._
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * Collection of options related to [[com.azure.search.documents.models.SearchOptions]] building
 * @param options case-insensitive options
 */

case class SearchOptionsBuilderImpl(override protected val options: CaseInsensitiveMap[String])
  extends SearchConfig(options)
    with ExtendableConfig[SearchOptionsBuilderImpl]
      with SearchOptionsBuilder {

  /**
   * Create a new config instance by upserting the underlying options (i.e., by either adding a new
   * key-value pair if such key does not exist or by replacing its value otherwise)
   * @param key key
   * @param value value
   * @return a new config instance
   */

  override def withOption(key: String, value: String): SearchOptionsBuilderImpl = {

    this.copy(
      options = options + (key, value)
    )
  }

  /**
   * Return the text to be searched when querying documents
   * @return the text to be searched when querying documents
   */

  private[read] def searchText: Option[String] = get(SearchOptionsBuilderImpl.SEARCH)

  /**
   * Return the set of index fields to select. If not provided, all retrievable fields will be selected
   * @return index fields to select
   */

  private[read] def select: Option[Seq[String]] = getAsList(SearchOptionsBuilderImpl.SELECT)

  /**
   * Get the Odata filter to apply on index documents. The filter must follow OData syntax
   * @return the filter to apply on search index documents
   */

  private[read] def filter: Option[String] = get(SearchOptionsBuilderImpl.FILTER)

  /**
   * Get the OData filter created by predicate pushdown. It should be combined with <code>filter</code> using AND
   * @return OData filter created by predicate pushdown
   */

  private[read] def pushedPredicate: Option[String] = get(SearchOptionsBuilderImpl.PUSHED_PREDICATE)

  /**
   * Combine <code>filter</code> and <code>pushedPredicate</code> into one OData filter, using <code>and</code>
   * @return a combined filter, which represents the logical <code>and</code> of the two filters
   */

  private[read] def combinedFilter: Option[String] = {

    val definedFilters = Seq(filter, pushedPredicate).collect {
      case Some(value) => value
    }

    // If empty, no filter should be defined
    if (definedFilters.isEmpty) {
      None
    } else {
      // If only one, use that
      if (definedFilters.size.equals(1)) {
        definedFilters.headOption
      } else {
        // Otherwise, combine them using AND
        Some(
          definedFilters.map {
            filter => s"($filter)"
          }.mkString(" and ")
        )
      }
    }
  }

  /**
   * Get the query type to use for querying documents
   * @return query type
   */

  private[read] def queryType: Option[QueryType] = {

    getAs(
      SearchOptionsBuilderImpl.QUERY_TYPE,
      name => Enums.unsafeValueOf[QueryType](
        name,
        (q, v) => q.name().equalsIgnoreCase(v)
      )
    )
  }

  /**
   * Get the search mode to use for querying documents
   * @return search mode
   */

  private[read] def searchMode: Option[SearchMode] = {

    getAs(
      SearchOptionsBuilderImpl.SEARCH_MODE,
      name => Enums.unsafeValueOf[SearchMode](
        name,
        (s, v) => s.name().equalsIgnoreCase(v)
      )
    )
  }

  /**
   * Get the facets to set when querying document
   * @return facets to use for querying documents
   */

  private[read] def facets: Option[Seq[String]] = getAsList(SearchOptionsBuilderImpl.FACETS, '|')

  /**
   * Get the list of fields to which text search should be limited
   * @return list of field to be used for text search
   */

  private[read] def searchFields: Option[Seq[String]] = getAsList(SearchOptionsBuilderImpl.SEARCH_FIELDS)

  /**
   * Adds a new filter to the existing filter condition.
   * If a filter already exists, the new filter is combined with the existing one using an 'AND' operation.
   * If no filter exists, the new filter becomes the sole filter condition.
   * @param other the new filter condition to be added.
   * @return this instance with an updated filter condition.
   */

  override def addFilter(other: String): SearchOptionsBuilderImpl = {

    val newFilterValue: String = filter.map {
      old => s"($old) and ($other)"
    }.getOrElse(other)

    withOption(SearchOptionsBuilderImpl.FILTER, newFilterValue)
  }

  /**
   * Add a facet to the search
   * @param facet facet to add
   * @return this instance with an added facet
   */

  override def addFacet(facet: String): SearchOptionsBuilderImpl = {

    val newFacetsValue: String = facets.map(_ :+ facet)
      .getOrElse(Seq(facet))
      .mkString("|")

    withOption(
      SearchOptionsBuilderImpl.FACETS,
      newFacetsValue
    )
  }

  /**
   * Builds and returns query Search options based on the current configuration
   * @return query options based on the current configuration
   */

  override def buildOptions(): SearchOptions = {

    new SearchOptions()
      .setFilter(combinedFilter)
      .setSelect(select)
      .setFacets(facets)
      .setQueryType(queryType)
      .setSearchMode(searchMode)
      .setSearchFields(searchFields)
  }
}

object SearchOptionsBuilderImpl {

  final val SEARCH = "search"
  final val FILTER = "filter"
  final val PUSHED_PREDICATE = "pushedPredicate"
  final val SELECT = "select"
  final val QUERY_TYPE = "queryType"
  final val SEARCH_MODE = "searchMode"
  final val FACETS = "facets"
  final val SEARCH_FIELDS = "searchFields"

  /**
   * Create an instance from a [[SearchConfig]]
   * @param config config instance
   * @return an instance of [[SearchOptionsBuilderImpl]]
   */

  def apply(config: SearchConfig): SearchOptionsBuilderImpl = {

    SearchOptionsBuilderImpl(
      CaseInsensitiveMap(config.toMap)
    )
  }
}
