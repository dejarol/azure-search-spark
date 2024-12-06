package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.LexicalAnalyzerName
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.config.SearchConfig
import com.github.jarol.azure.search.spark.sql.connector.core.schema.SearchFieldAction
import com.github.jarol.azure.search.spark.sql.connector.core.utils.Enums

import java.util.stream.Collectors

/**
 * Configuration for creating actions related to Search field analyzers
 * @param alias analyzer alias
 * @param name lexical analyzer name
 * @param `type` analyzer type
 * @param fields fields for which the analyzer should be set
 */

case class AnalyzerConfig(
                           private[write] val alias: String,
                           private[write] val name: LexicalAnalyzerName,
                           private[write] val `type`: SearchFieldAnalyzerType,
                           private[write] val fields: Seq[String]
                         ) {

  /**
   * Get the set of actions
   * @return
   */

  def actions: Seq[(String, SearchFieldAction)] = {

    fields.map {
      field => (
        field,
        SearchFieldActions.forSettingAnalyzer(`type`, name)
      )
    }
  }
}

object AnalyzerConfig {

  /**
   * Create a collection of [[AnalyzerConfig]](s)
   * <br>
   * A non-empty collection will be returned if
   *  - a key <code>aliases</code>, having a list of strings as value, exists
   *  - at least for one of the aliases, a valid [[AnalyzerConfig]] could be retrieved
   * @param config configuration related to analyzers
   * @return a collection of [[AnalyzerConfig]]
   */

  def createCollection(config: SearchConfig): Option[Seq[AnalyzerConfig]] = {

    // Get the list of aliases
    config.getAsList(WriteConfig.ALIASES_SUFFIX).flatMap {
      aliases =>

        // For each alias, retrieve the analyzer configuration
        val validAnalyzerConfigs: Seq[AnalyzerConfig] = aliases.map {
          alias => createInstance(alias, config.getAllWithPrefix(s"$alias."))
        }.collect {
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
   * A non-empty instance will be returned only if
   *  - a key <code>name</code> exists and is a valid lexical analyzer name
   *  - a key <code>type</code> exists and is a valid analyzer type
   *  - a key <code>onFields</code> exists and contains a list of comma-separated field names
   * @param alias analyzer alias within the Spark configuration
   * @param config configuration wrapping information about all analyzers
   * @return an optional instance
   */

  private[write] def createInstance(
                                     alias: String,
                                     config: SearchConfig
                                   ): Option[AnalyzerConfig] = {

    for {
      name <- config.getAs[LexicalAnalyzerName](WriteConfig.NAME_SUFFIX, resolveLexicalAnalyzer)
      analyzerType <- config.getAs[SearchFieldAnalyzerType](WriteConfig.TYPE_SUFFIX, resolveAnalyzerType)
      fields <- config.getAsList(WriteConfig.ON_FIELDS_SUFFIX)
    } yield AnalyzerConfig(alias, name, analyzerType, fields)
  }

  /**
   * Resolve an analyzer by name, or throw an exception
   * @param name name to resolve
   * @throws NoSuchElementException if the name does not match nay analyzer
   * @return the matching analyzer
   */

  @throws[NoSuchElementException]
  private[write] def resolveLexicalAnalyzer(name: String): LexicalAnalyzerName = {

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

  /**
   * Resolve a string as a value of [[SearchFieldAnalyzerType]], using a case-insensitive match on
   * the enum name and description
   * @param name name to resolve
   * @return the matching enum value
   */

  private[write] def resolveAnalyzerType(name: String): SearchFieldAnalyzerType = {

    Enums.unsafeValueOf[SearchFieldAnalyzerType](
      name,
      (e, v) => e.name().equalsIgnoreCase(v) ||
        e.description().equalsIgnoreCase(v)
    )
  }
}
