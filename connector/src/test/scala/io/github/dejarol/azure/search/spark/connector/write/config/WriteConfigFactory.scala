package io.github.dejarol.azure.search.spark.connector.write.config

import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.{JsonMixIns, SearchAPIModelFactory}

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

  protected final def indexOptionKey(suffix: String): String = WriteConfig.INDEX_ATTRIBUTES_PREFIX + suffix

  /**
   * Create a raw configuration object that includes options for many analyzers
   * @param analyzers analyzer map
   * @return a raw configuration object
   */

  protected final def configForAnalyzers(analyzers: Seq[AnalyzerConfig]): Map[String, String] = {

    analyzers.flatMap {
      cfg => JavaScalaConverters.listToSeq(cfg.getFields).map {
        field =>

          val property = cfg.getType match {
            case SearchFieldAnalyzerType.ANALYZER => "analyzer"
            case SearchFieldAnalyzerType.SEARCH_ANALYZER => "searchAnalyzer"
            case SearchFieldAnalyzerType.INDEX_ANALYZER => "indexAnalyzer"
          }

          val json =
            s"""
               |{
               |  "$property": "${cfg.getName}"
               |}
               |""".stripMargin
          (
            fieldOptionKey(field),
            json
          )
      }
    }.toMap
  }
}
