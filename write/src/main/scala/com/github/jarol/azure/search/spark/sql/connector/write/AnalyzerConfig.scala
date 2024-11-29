package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.LexicalAnalyzerName
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.config.SearchConfig
import com.github.jarol.azure.search.spark.sql.connector.core.schema.{SearchFieldAction, SearchFieldActions}

import java.util.stream.Collectors

/**
 * Configuration for creating actions related to Search field analyzers
 * @param name analyzer name
 * @param fields fields for which the analyzer should be set
 * @param supplier function for creating field actions
 */

case class AnalyzerConfig(
                           private[write] val name: LexicalAnalyzerName,
                           private[write] val fields: Seq[String],
                           private val supplier: LexicalAnalyzerName => SearchFieldAction
                         ) {

  /**
   * Get the set of actions
   * @return
   */

  def actions: Seq[(String, SearchFieldAction)] = fields.map((_, supplier(name)))
}

object AnalyzerConfig {

  /**
   * Create a collection of [[AnalyzerConfig]] for analyzers
   * @param config configuration to use for extracting analyzers information
   * @return a collection of configurations for analyzers
   */

  def forAnalyzers(config: SearchConfig): Option[Seq[AnalyzerConfig]] = {

    createCollection(
      config,
      SearchFieldActions.forSettingAnalyzer
    )
  }

  /**
   * Create a collection of [[AnalyzerConfig]] for Search analyzers
   * @param config configuration to use for extracting analyzers information
   * @return a collection of configurations for Search analyzers
   */

  def forSearchAnalyzers(config: SearchConfig): Option[Seq[AnalyzerConfig]] = {

    createCollection(
      config,
      SearchFieldActions.forSettingSearchAnalyzer
    )
  }

  /**
   * Create a collection of [[AnalyzerConfig]] for index analyzers
   * @param config configuration to use for extracting analyzers information
   * @return a collection of configurations for index analyzers
   */

  def forIndexAnalyzers(config: SearchConfig): Option[Seq[AnalyzerConfig]] = {

    createCollection(
      config,
      SearchFieldActions.forSettingIndexAnalyzer
    )
  }

  /**
   * Create a collection of [[AnalyzerConfig]](s)
   * <br>
   * A non-empty collection will be returned if
   *  - a key <code>aliases</code>, having a list of strings as value, exists
   *  - at least for one of the aliases, a valid [[AnalyzerConfig]] could be retrieved
   * @param config configuration related to analyzers
   * @param supplier function for creating a [[SearchFieldAction]] from a [[LexicalAnalyzerName]]
   * @return a collection of [[AnalyzerConfig]]
   */

  private[write] def createCollection(
                                       config: SearchConfig,
                                       supplier: LexicalAnalyzerName => SearchFieldAction
                                     ): Option[Seq[AnalyzerConfig]] = {

    // Get the list of aliases
    config.getAsList(WriteConfig.ALIASES_CONFIG).flatMap {
      aliases =>

        // For each alias, retrieve the analyzer configuration
        val validAnalyzerConfigs: Seq[AnalyzerConfig] = aliases
          .map(createInstance(_, config, supplier))
          .collect {
            case Some(value) => value
          }

        // If some valid configuration exists, create the instance
        if (validAnalyzerConfigs.isEmpty) {
          None
        } else {
          Some(validAnalyzerConfigs)
        }
    }
  }

  /**
   * Create an instance from a configuration object
   * <br>
   * Given an alias <code>a1</code>, a non-empty instance will be returned only if
   *  - a key <code>a1.type</code> exists and is a valid analyzer name
   *  - a key <code>a1.onFields</code> exists and contains a list of comma-separated field names
   * @param alias analyzer alias
   * @param config configuration wrapping information about all analyzers
   * @param supplier supplier for creating [[SearchFieldAction]](s) from a resolver analyzer
   * @return an optional instance
   */

  private[write] def createInstance(
                                     alias: String,
                                     config: SearchConfig,
                                     supplier: LexicalAnalyzerName => SearchFieldAction
                                   ): Option[AnalyzerConfig] = {

    for {
      name <- config.getAs[LexicalAnalyzerName](s"$alias.${WriteConfig.TYPE_SUFFIX}", resolveAnalyzer)
      fields <- config.getAsList(s"$alias.${WriteConfig.ON_FIELDS_SUFFIX}")
    } yield AnalyzerConfig(name, fields, supplier)
  }

  /**
   * Resolve an analyzer by name, or throw an exception
   * @param name name to resolve
   * @throws NoSuchElementException if the name does not match nay analyzer
   * @return the matching analyzer
   */

  @throws[NoSuchElementException]
  private[write] def resolveAnalyzer(name: String): LexicalAnalyzerName = {

    // Get all available values
    val availableValues: Seq[LexicalAnalyzerName] = JavaScalaConverters.listToSeq(
      LexicalAnalyzerName.values().stream().collect(
        Collectors.toList[LexicalAnalyzerName]()
      )
    )

    // Find matching value or throw an exception
    availableValues.find(
      _.toString.equalsIgnoreCase(name)
    ) match {
      case Some(value) => value
      case None =>
        val description = availableValues.map(_.toString).mkString("[", ",", "]")
        throw new NoSuchElementException(s"Analyzer '$name' does not exist. It should be one among $description")
    }
  }
}
