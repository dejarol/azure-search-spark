package io.github.jarol.azure.search.spark.connector.read

import com.azure.search.documents.models.{QueryType, SearchMode, SearchOptions}

import scala.language.implicitConversions

/**
 * Utility class for interacting with [[SearchOptions]] using some standard Scala types
 * @param original original options
 */

case class SearchOptionsOperations(private val original: SearchOptions) {

  /**
   * Update this instance's options using a setter function if given value is defined
   * @param maybeValue configuration value
   * @param setter function that should combine the defined value and this instance in order to create a new, updated instance
   * @tparam T input value type
   * @return this options if the config is empty, an updated version otherwise
   */

  private def maybeUpdate[T](maybeValue: Option[T], setter: (SearchOptions, T) => SearchOptions): SearchOptions = {

    maybeValue match {
      case Some(value) => setter(original, value)
      case None => original
    }
  }

  /**
   * Set the filter, if defined
   * @param filter filter
   * @return this options updated with a filter (if defined)
   */

  def setFilter(filter: Option[String]): SearchOptions = {

    maybeUpdate[String](
      filter,
      (o, f) => o.setFilter(f)
    )
  }

  /**
   * Set the index fields to select, if defined
   * @param select filter
   * @return this options updated with index fields to select (if defined)
   */

  def setSelect(select: Option[Seq[String]]): SearchOptions = {

    maybeUpdate[Seq[String]](
      select,
      (o, s) => o.setSelect(s: _*)
    )
  }

  /**
   * Set the query type, if defined
   * @param queryType query type
   * @return this options updated with a query type (if defined)
   */

  def setQueryType(queryType: Option[QueryType]): SearchOptions = {

    maybeUpdate[QueryType](
      queryType,
      (o, q) => o.setQueryType(q)
    )
  }

  /**
   * Set the facets, if defined
   * @param facets facets
   * @return this options updated with some facets (if defined)
   */

  def setFacets(facets: Option[Seq[String]]): SearchOptions = {

    maybeUpdate[Seq[String]](
      facets,
      (o, f) => o.setFacets(f: _*)
    )
  }

  /**
   * Set the search mode, if defined
   * @param mode search mode
   * @return this options updated with a search mode (if defined)
   */

  def setSearchMode(mode: Option[SearchMode]): SearchOptions = {

    maybeUpdate[SearchMode](
      mode,
      (o, s) => o.setSearchMode(s)
    )
  }

  /**
   * Set the search fields, if defined
   * @param fields search fields
   * @return this options updated with some search fields
   */

  def setSearchFields(fields: Option[Seq[String]]): SearchOptions = {

    maybeUpdate[Seq[String]](
      fields,
      (o, f) => o.setSearchFields(f: _*)
    )
  }
}

object SearchOptionsOperations {

  /**
   * Implicit conversion from [[SearchOptions]] to this wrapper
   * @param original original options
   * @return an instance of [[SearchOptionsOperations]]
   */

  implicit def toOperations(original: SearchOptions): SearchOptionsOperations = SearchOptionsOperations(original)
}