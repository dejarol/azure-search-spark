package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.models.SearchOptions
import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig

abstract class AbstractSearchPartition(protected val readConfig: ReadConfig)
  extends SearchPartition {

  /**
   * Maybe update an initial set of [[SearchOptions]]
   * @param original original options
   * @param config optional value to use for updating the options
   * @param update update function to apply
   * @tparam T optional value type
   * @return either the original options if the value is empty, or an updated version
   */

  protected final def maybeUpdate[T](original: SearchOptions,
                                     config: Option[T],
                                     update: (SearchOptions, T) => SearchOptions): SearchOptions = {

    config match {
      case Some(value) => update(original, value)
      case None => original
    }
  }

  /**
   * Maybe set a filter for given [[SearchOptions]]
   * @param original original search options
   * @param filter filter to apply
   * @return input options with a filter (if defined)
   */

  protected final def setFilter(original: SearchOptions, filter: Option[String]): SearchOptions = maybeUpdate(original, filter, _.setFilter(_))
}
