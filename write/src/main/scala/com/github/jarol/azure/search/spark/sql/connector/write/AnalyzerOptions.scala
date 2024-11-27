package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.LexicalAnalyzerName
import com.github.jarol.azure.search.spark.sql.connector.core.config.SearchConfig
import com.github.jarol.azure.search.spark.sql.connector.core.schema.{SearchFieldAction, SearchFieldActions}

case class AnalyzerOptions(
                            private val analyzerConfigs: Seq[AnalyzerConfig],
                            private val supplier: LexicalAnalyzerName => SearchFieldAction
                          )
  extends FieldActionsFactory {

  override def actions: Seq[(String, SearchFieldAction)] = analyzerConfigs.flatMap(_.actions)
}

object AnalyzerOptions {

  private def fromConfig(
                          config: SearchConfig,
                          supplier: LexicalAnalyzerName => SearchFieldAction
                        ): Option[AnalyzerOptions] = {

    config.getAsList(WriteConfig.ALIASES_CONFIG).flatMap {
      aliases =>

        val analyzerConfigs: Seq[AnalyzerConfig] = aliases
          .map(AnalyzerConfig.fromConfig(_, config, supplier))
          .collect {
            case Some(value) => value
          }

        if (analyzerConfigs.isEmpty) {
          None
        } else {
          Some(
            AnalyzerOptions(
              analyzerConfigs,
              supplier
            )
          )
        }
    }
  }

  def forAnalyzers(config: SearchConfig): Option[AnalyzerOptions] = fromConfig(config, SearchFieldActions.forSettingAnalyzer)

  def forSearchAnalyzers(config: SearchConfig): Option[AnalyzerOptions] = fromConfig(config, SearchFieldActions.forSettingSearchAnalyzer)

  def forIndexAnalyzers(config: SearchConfig): Option[AnalyzerOptions] = fromConfig(config, SearchFieldActions.forSettingIndexAnalyzer)
}
