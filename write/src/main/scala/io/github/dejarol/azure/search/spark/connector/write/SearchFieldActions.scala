package io.github.dejarol.azure.search.spark.connector.write

import com.azure.search.documents.indexes.models.{LexicalAnalyzerName, SearchField}
import io.github.dejarol.azure.search.spark.connector.core.schema.{SearchFieldAction, SearchFieldFeature}
import io.github.dejarol.azure.search.spark.connector.write.config.SearchFieldAnalyzerType

/**
 * Collection of methods for creating [[SearchFieldAction]](s)
 */

object SearchFieldActions {

  /**
   * Action for enabling a feature
   * @param feature feature to enable
   */

  private case class EnableFeature(private val feature: SearchFieldFeature)
    extends SearchFieldAction {
    override final def apply(field: SearchField): SearchField = {
      feature.enableOnField(field)
    }
  }

  /**
   * Action for disabling a feature
   * @param feature feature to disable
   */

  private case class DisableFeature(private val feature: SearchFieldFeature)
    extends SearchFieldAction {
    override def apply(field: SearchField): SearchField = {
      feature.disableOnField(field)
    }
  }

  /**
   * Action for setting an analyzer
   * @param analyzerType analyzer type
   * @param lexicalAnalyzerName lexical analyzer to set
   */

  private case class SetAnalyzer(
                                  private val analyzerType: SearchFieldAnalyzerType,
                                  private val lexicalAnalyzerName: LexicalAnalyzerName
                                )
    extends SearchFieldAction {
    override def apply(field: SearchField): SearchField = {
      analyzerType.setOnField(field, lexicalAnalyzerName)
    }
  }

  /**
   * Get an action for enabling a feature
   * @param feature feature to enable
   * @return an action for enabling a feature
   */

  final def forEnablingFeature(feature: SearchFieldFeature): SearchFieldAction = EnableFeature(feature)

  /**
   * Get an action for disabling a feature
   * @param feature feature to disable
   * @return an action for disabling a feature
   */

  final def forDisablingFeature(feature: SearchFieldFeature): SearchFieldAction = DisableFeature(feature)

  /**
   * Get an action for setting an analyzer
   * @param analyzerType analyzer type
   * @param lexicalAnalyzerName lexical analyzer to set
   * @return an action for setting a lexical analyzer
   */

  final def forSettingAnalyzer(
                                analyzerType: SearchFieldAnalyzerType,
                                lexicalAnalyzerName: LexicalAnalyzerName
                              ): SearchFieldAction = {

    SetAnalyzer(
      analyzerType,
      lexicalAnalyzerName
    )
  }
}
