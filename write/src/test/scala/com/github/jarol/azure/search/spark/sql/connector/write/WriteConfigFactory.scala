package com.github.jarol.azure.search.spark.sql.connector.write

import com.github.jarol.azure.search.spark.sql.connector.core.{JsonMixIns, SearchAPIModelFactory}

/**
 * Mix-in trait for creating raw configuration objects for write operations
 */

trait WriteConfigFactory
  extends JsonMixIns
    with SearchAPIModelFactory {

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
   * Create a raw configuration object that includes options for many analyzers
   * @param analyzers analyzer map
   * @return a raw configuration object
   */

  protected final def rawConfigForAnalyzers(analyzers: Seq[AnalyzerConfig]): Map[String, String] = {

    Map(
      fieldOptionKey(WriteConfig.ANALYZERS_CONFIG) -> writeValueAs[Seq[AnalyzerConfig]](analyzers)
    )
  }
}
