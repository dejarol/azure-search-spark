package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.{LexicalAnalyzerName, SearchField}

/**
 * Support trait for test that deal with field analyzers
 */

trait SearchFieldAnalyzerType {

  /**
   * Retrieve the analyzer related to this instance's type from a field
   * @param field Search field
   * @return the field's analyzer related to this instance's type
   */

  def getAnalyzerFrom(field: SearchField): LexicalAnalyzerName

  /**
   * Get the prefix related to this instance's analyzer type
   * @return prefix to be used on configuration creation
   */

  def prefix: String

  /**
   * Create a raw configuration object for a collection of analyzers
   * @param analyzer analyzer to set
   * @param onFields map with keys being analyzer aliases and values being the list of fields on which to set the analyzer
   * @return a raw configuration for multiple analyzers
   */

  final def rawConfigForAnalyzers(
                                   analyzer: LexicalAnalyzerName,
                                   onFields: Map[String, Seq[String]]
                                 ): Map[String, String] = {

    // Base configuration
    val aliasesMap = Map(
      s"${WriteConfig.FIELD_OPTIONS_PREFIX}$prefix${WriteConfig.ALIASES_CONFIG}" -> onFields.keys.mkString(","),
    )

    onFields.foldLeft(aliasesMap) {
      case (map, (alias, fields)) =>

        // Enrich the existing configuration with the current analyzer configuration
        map ++ rawConfigForAnalyzer(
          analyzer,
          alias,
          fields
        )
    }
  }

  /**
   * Create a raw configuration object for a single analyzer
   * <br>
   * The output map will contain the following keys
   *  - analyzer type
   *  - list of fields on which the analyzer should be set
   * @param analyzer analyzer
   * @param alias analyzer alias
   * @param onFields fields on which the analyzer should be set
   * @return a raw configuration for an analyzer
   */

  final def rawConfigForAnalyzer(
                                  analyzer: LexicalAnalyzerName,
                                  alias: String,
                                  onFields: Seq[String]
                                ): Map[String, String] = {

    Map(
      s"${WriteConfig.FIELD_OPTIONS_PREFIX}$prefix$alias.${WriteConfig.TYPE_SUFFIX}" -> analyzer.toString,
      s"${WriteConfig.FIELD_OPTIONS_PREFIX}$prefix$alias.${WriteConfig.ON_FIELDS_SUFFIX}" -> onFields.mkString(",")
    )
  }
}

object SearchFieldAnalyzerType {

  /**
   * Type related to both Search and index analyzer
   */

  case object SEARCH_AND_INDEX extends SearchFieldAnalyzerType {
    override def getAnalyzerFrom(field: SearchField): LexicalAnalyzerName = field.getAnalyzerName
    override def prefix: String = WriteConfig.ANALYZERS_PREFIX
  }

  /**
   * Type related to Search analyzer
   */

  case object ONLY_SEARCH extends SearchFieldAnalyzerType {
    override def getAnalyzerFrom(field: SearchField): LexicalAnalyzerName = field.getSearchAnalyzerName
    override def prefix: String = WriteConfig.SEARCH_ANALYZERS_PREFIX
  }

  /**
   * Type related to index analyzer
   */

  case object ONLY_INDEX extends SearchFieldAnalyzerType {
    override def getAnalyzerFrom(field: SearchField): LexicalAnalyzerName = field.getIndexAnalyzerName
    override def prefix: String = WriteConfig.INDEX_ANALYZERS_PREFIX
  }
}
