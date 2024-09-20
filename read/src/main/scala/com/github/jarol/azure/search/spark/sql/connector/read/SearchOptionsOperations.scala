package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.models.SearchOptions

import scala.language.implicitConversions

/**
 * Wrapper class for interacting with [[SearchOptions]] using some standard Scala types
 * @param original original options
 */

case class SearchOptionsOperations(private val original: SearchOptions) {

  /**
   * Update this instance's options using an update function if given configuration is defined
   * @param config configuration value
   * @param update update function
   * @tparam T input value type
   * @return the original options if the config is empty, an updated version otherwise
   */

  private def maybeUpdate[T](config: Option[T], update: (SearchOptions, T) => SearchOptions): SearchOptions = {

    config match {
      case Some(value) => update(original, value)
      case None => original
    }
  }

  /**
   * Set the filter, if defined
   * @param filter filter
   * @return this instance's options updated with a filter (if defined)
   */

  def setFilter(filter: Option[String]): SearchOptions = maybeUpdate[String](filter, (o, f) => o.setFilter(f))

  /**
   * Set the index fields to select, if defined
   * @param select filter
   * @return this instance's options updated with index fields to select (if defined)
   */

  def setSelect(select: Option[Seq[String]]): SearchOptions = {

    maybeUpdate[Seq[String]](
      select,
      (o, s) => o.setSelect(s: _*)
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