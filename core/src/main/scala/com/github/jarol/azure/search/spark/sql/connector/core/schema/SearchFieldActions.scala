package com.github.jarol.azure.search.spark.sql.connector.core.schema

import com.azure.search.documents.indexes.models.{LexicalAnalyzerName, SearchField}

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
   * @param analyzer analyzer to set
   */

  private case class SetAnalyzer(private val analyzer: LexicalAnalyzerName)
    extends SearchFieldAction {
    override def apply(field: SearchField): SearchField = {
      field.setAnalyzerName(analyzer)
    }
  }

  /**
   * Action for setting a Search analyzer
   * @param analyzer Search analyzer to add
   */

  private case class SetSearchAnalyzer(private val analyzer: LexicalAnalyzerName)
    extends SearchFieldAction {
    override def apply(field: SearchField): SearchField = {
      field.setSearchAnalyzerName(analyzer)
    }
  }

  /**
   * Action for setting an index analyzer
   * @param analyzer index analyzer to add
   */

  private case class SetIndexAnalyzer(private val analyzer: LexicalAnalyzerName)
    extends SearchFieldAction {
    override def apply(field: SearchField): SearchField = {
      field.setIndexAnalyzerName(analyzer)
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
   * @param analyzerName analyzer to set
   * @return an action for setting an analyzer
   */

  final def forSettingAnalyzer(analyzerName: LexicalAnalyzerName): SearchFieldAction = SetAnalyzer(analyzerName)

  /**
   * Get an action for setting a Search analyzer
   * @param analyzerName Search analyzer to set
   * @return an action for setting a Search analyzer
   */

  final def forSettingSearchAnalyzer(analyzerName: LexicalAnalyzerName): SearchFieldAction = SetSearchAnalyzer(analyzerName)

  /**
   * Get an action for setting an index analyzer
   * @param analyzerName index analyzer to set
   * @return an action for setting an index analyzer
   */

  final def forSettingIndexAnalyzer(analyzerName: LexicalAnalyzerName): SearchFieldAction = SetIndexAnalyzer(analyzerName)
}
