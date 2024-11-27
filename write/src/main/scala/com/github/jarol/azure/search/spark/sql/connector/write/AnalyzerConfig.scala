package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.LexicalAnalyzerName
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.config.SearchConfig
import com.github.jarol.azure.search.spark.sql.connector.core.schema.SearchFieldAction

import java.util.stream.Collectors

/**
 * Configuration for creating actions related to Search field analyzers
 * @param name analyzer name
 * @param fields fields for which the analyzer should be set
 * @param supplier function for creating field actions
 */

case class AnalyzerConfig(
                           private val name: LexicalAnalyzerName,
                           private val fields: Seq[String],
                           private val supplier: LexicalAnalyzerName => SearchFieldAction
                         )
  extends FieldActionsFactory {

  override def actions: Seq[(String, SearchFieldAction)] = fields.map((_, supplier(name)))
}

object AnalyzerConfig {

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

  def fromConfig(
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
