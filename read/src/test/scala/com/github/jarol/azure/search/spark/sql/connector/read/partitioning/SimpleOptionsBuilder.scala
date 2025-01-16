package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.models.SearchOptions
import com.github.jarol.azure.search.spark.sql.connector.read.SearchOptionsBuilder

/**
 * A simple implementation of options builder
 * <br>
 * It simply cumulates filters and facets
 * @param cumulatedFilters cumulated filters
 * @param cumulatedFacets cumulated facets
 */

case class SimpleOptionsBuilder(
                                 private val cumulatedFilters: Seq[String],
                                 private val cumulatedFacets: Seq[String]
                               )
  extends SearchOptionsBuilder {

  private lazy val filter: String = if (cumulatedFilters.isEmpty) {
    null
  } else {
    if (cumulatedFilters.size.equals(1)) {
      cumulatedFilters.head
    } else {
      cumulatedFilters.map {
        filter => s"($filter)"
      }.mkString(" and ")
    }
  }

  override def buildOptions(): SearchOptions = {

    new SearchOptions()
      .setFilter(filter)
      .setFacets(cumulatedFacets: _*)
  }

  override def addFilter(other: String): SearchOptionsBuilder = this.copy(cumulatedFilters = cumulatedFilters :+ other)
  override def addFacet(facet: String): SearchOptionsBuilder = this.copy(cumulatedFacets = cumulatedFacets :+ facet)
}

object SimpleOptionsBuilder {



  /**
   * Create an empty instance (i.e. no filter or facet has been cumulated)
   * @return an empty instance
   */

  def empty(): SimpleOptionsBuilder = {

    SimpleOptionsBuilder(
      Seq.empty,
      Seq.empty
    )
  }

  /**
   * Create an instance with just one cumulated filter
   * @param filter filter
   * @return a builder instance
   */

  def withFilter(filter: String): SimpleOptionsBuilder = {

    SimpleOptionsBuilder(
      Seq(filter),
      Seq.empty
    )
  }

  /**
   * Create an instance, cumulating the filer if defined
   * @param filter optional filter
   * @return a builder instance
   */

  def maybeWithFilter(filter: Option[String]): SimpleOptionsBuilder = {

    filter
      .map(withFilter)
      .getOrElse(empty())
  }
}
