package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.LexicalAnalyzerName

/**
 * Mix-in trait for creating raw configuration objects for write operations
 */

trait WriteConfigFactory {

  /**
   * Create a write config key related to search field creation options
   * @param suffix suffix to append
   * @return config key for field creation
   */

  protected final def fieldOptionKey(suffix: String): String = WriteConfig.FIELD_OPTIONS_PREFIX + suffix

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
                                            analyzerName: LexicalAnalyzerName,
                                            analyzerType: SearchFieldAnalyzerType,
                                            onFields: Seq[String]
                                          ): Map[String, String] = {

    Map(
      analyzerOptionKey(s"$alias.${WriteConfig.NAME_SUFFIX}") -> analyzerName.toString,
      analyzerOptionKey(s"$alias.${WriteConfig.TYPE_SUFFIX}") -> analyzerType.name(),
      analyzerOptionKey(s"$alias.${WriteConfig.ON_FIELDS_SUFFIX}") -> onFields.mkString(",")
    )
  }
}
