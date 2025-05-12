package io.github.dejarol.azure.search.spark.connector.write

import com.azure.search.documents.indexes.models.{LexicalAnalyzerName, SearchField}
import io.github.dejarol.azure.search.spark.connector.core.schema.{SearchFieldAction, SearchFieldFeature}
import io.github.dejarol.azure.search.spark.connector.write.config.SearchFieldAnalyzerType

/**
 * Collection of methods for creating [[io.github.dejarol.azure.search.spark.connector.core.schema.SearchFieldAction]](s)
 */

object SearchFieldActions {

  /**
   * Action for enabling a feature
   * @param feature feature to enable
   */

  private case class EnableFeature(private val feature: SearchFieldFeature)
    extends SearchFieldAction {
    override def description(): String = s"ENABLE_${feature.name()}"
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
    override def description(): String = s"DISABLE_${feature.name()}"
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
    override def description(): String = s"SET_${analyzerType.name()}"
    override def apply(field: SearchField): SearchField = {
      analyzerType.setOnField(field, lexicalAnalyzerName)
    }
  }

  /**
   * Action for setting a vector search profile
   * @param profile profile to set
   */

  private case class SetVectorSearchProfile(private val profile: String)
    extends SearchFieldAction {
    override def description(): String = "SET_VECTOR_SEARCH_PROFILE"
    override def apply(field: SearchField): SearchField = {
      field.setVectorSearchProfileName(profile)
    }
  }

  /**
   * Action for applying many actions at once
   * @param actions actions to apply
   */

  private case class FoldActions(private val actions: Seq[SearchFieldAction])
    extends SearchFieldAction {

    override def description(): String = {

      s"ACTIONS(" +
        s"${actions.map(_.description()).mkString(", ")}" +
        s")"
    }

    override def apply(field: SearchField): SearchField = {

      actions.foldLeft(field) {
        case (field, action) =>
          action.apply(field)
      }
    }
  }

  /**
   * Creates an action for enabling or disabling a feature
   * @param feature feature to enable or disable
   * @param flag true for enabling, false for disabling
   * @return an action for enabling or disabling a feature
   */

  final def forEnablingOrDisablingFeature(feature: SearchFieldFeature, flag: Boolean): SearchFieldAction = {

    if (flag) {
      EnableFeature(feature)
    } else {
      DisableFeature(feature)
    }
  }

  /**
   * Gets an action for enabling a feature
   * @param feature feature to enable
   * @return an action for enabling a feature
   */

  final def forEnablingFeature(feature: SearchFieldFeature): SearchFieldAction = EnableFeature(feature)

  /**
   * Gets an action for disabling a feature
   * @param feature feature to disable
   * @return an action for disabling a feature
   */

  final def forDisablingFeature(feature: SearchFieldFeature): SearchFieldAction = DisableFeature(feature)

  /**
   * Gets an action for setting an analyzer
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

  final def forSettingAnalyzer(name: LexicalAnalyzerName): SearchFieldAction = {

    SetAnalyzer(
      SearchFieldAnalyzerType.ANALYZER,
      name
    )
  }

  final def forSettingIndexAnalyzer(name: LexicalAnalyzerName): SearchFieldAction = {

    SetAnalyzer(
      SearchFieldAnalyzerType.INDEX_ANALYZER,
      name
    )
  }

  final def forSettingSearchAnalyzer(name: LexicalAnalyzerName): SearchFieldAction = {

    SetAnalyzer(
      SearchFieldAnalyzerType.SEARCH_ANALYZER,
      name
    )
  }

  /**
   * Gets an action for setting attribute <code>vectorSearchProfile</code> on a Search field
   * @param profile profile to set
   * @return an action for setting the vector search profile
   */

  final def forSettingVectorSearchProfile(profile: String): SearchFieldAction = SetVectorSearchProfile(profile)

  /**
   * Gets an action for applying many actions at once
   * @param actions actions to apply on a field
   * @return an action for applying many others
   */

  final def forFoldingActions(actions: Seq[SearchFieldAction]): SearchFieldAction = FoldActions(actions)
}
