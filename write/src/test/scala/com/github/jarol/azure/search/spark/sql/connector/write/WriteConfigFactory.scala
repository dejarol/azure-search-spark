package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.LexicalAnalyzerName

/**
 * Mix-in trait for creating raw configuration objects for write operations
 */

trait WriteConfigFactory {

  protected final type AnalyzerMap = Map[String, (SearchFieldAnalyzerType, LexicalAnalyzerName, Seq[String])]

  /**
   * Create a write config key related to search field creation options
   * @param suffix suffix to append
   * @return config key for field creation
   */

  protected final def fieldOptionKey(suffix: String): String = WriteConfig.FIELD_OPTIONS_PREFIX + suffix

  /**
   * Create a write config key related to search index creation options
   * @param suffix suffix to append
   * @return config key for index creation
   */

  protected final def indexOptionKey(suffix: String): String = WriteConfig.INDEX_OPTIONS_PREFIX + suffix

  /**
   * Create a write config key related to search field analyzer options
   * @param suffix suffix to append
   * @return config key for field analyzers
   */

  protected final def analyzerOptionKey(suffix: String): String = fieldOptionKey(WriteConfig.ANALYZERS_PREFIX + suffix)

  /**
   * Create a raw configuration object for a single analyzer
   * <br>
   * The output map will contain the following keys
   *  - analyzer type
   *  - list of fields on which the analyzer should be set
   * @param analyzerName analyzer
   * @param alias analyzer alias
   * @param onFields fields on which the analyzer should be set
   * @return a raw configuration for an analyzer
   */

  protected final def rawConfigForAnalyzer(
                                            alias: String,
                                            analyzerType: SearchFieldAnalyzerType,
                                            analyzerName: LexicalAnalyzerName,
                                            onFields: Seq[String]
                                          ): Map[String, String] = {

    Map(
      analyzerOptionKey(s"$alias.${WriteConfig.NAME_SUFFIX}") -> analyzerName.toString,
      analyzerOptionKey(s"$alias.${WriteConfig.TYPE_SUFFIX}") -> analyzerType.name(),
      analyzerOptionKey(s"$alias.${WriteConfig.ON_FIELDS_SUFFIX}") -> onFields.mkString(",")
    )
  }

  /**
   * Create a raw configuration object that includes options for many analyzers
   * @param analyzers analyzer map
   * @return a raw configuration object
   */

  protected final def rawConfigForAnalyzers(analyzers: AnalyzerMap): Map[String, String] = {

    val baseMap = Map(
      analyzerOptionKey(WriteConfig.ALIASES_SUFFIX) -> analyzers.keySet.mkString(",")
    )

    analyzers.foldLeft(baseMap) {
      case (map, (alias, (analyzerType, name, fields))) =>
        map ++ rawConfigForAnalyzer(alias, analyzerType, name, fields)
    }
  }
}
