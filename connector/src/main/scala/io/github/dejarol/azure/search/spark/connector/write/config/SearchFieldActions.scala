package io.github.dejarol.azure.search.spark.connector.write.config

import com.azure.search.documents.indexes.models.{LexicalAnalyzerName, SearchField}
import io.github.dejarol.azure.search.spark.connector.core.schema.{SearchFieldAction, SearchFieldFeature}

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
   * Action for setting an analyzer for both searching and indexing
   * @param name analyzer to set
   */

  private case class SetAnalyzer(private val name: LexicalAnalyzerName)
    extends SearchFieldAction {
    override def apply(field: SearchField): SearchField = {
      field.setAnalyzerName(name)
    }
  }

  /**
   * Action for setting only the search analyzer
   * @param name analyzer to set
   */

  private case class SetSearchAnalyzer(private val name: LexicalAnalyzerName)
    extends SearchFieldAction {
    override def apply(field: SearchField): SearchField = {
      field.setSearchAnalyzerName(name)
    }
  }

  /**
   * Action for setting only the index analyzer
   * @param name analyzer to set
   */

  private case class SetIndexAnalyzer(private val name: LexicalAnalyzerName)
    extends SearchFieldAction {
    override def apply(field: SearchField): SearchField = {
      field.setIndexAnalyzerName(name)
    }
  }

  /**
   * Action for setting a vector search profile
   * @param profile profile to set
   * @since 0.10.0
   */

  private case class SetVectorSearchProfile(private val profile: String)
    extends SearchFieldAction {
    override def apply(field: SearchField): SearchField = {
      field.setVectorSearchProfileName(profile)
    }
  }

  /**
   * Action for setting synonyms
   * @param synonyms synonyms to set
   * @since 0.10.2
   */

  private case class SetSynonyms(private val synonyms: Seq[String])
    extends SearchFieldAction {
    override def apply(field: SearchField): SearchField = {
      field.setSynonymMapNames(synonyms: _*)
    }
  }

  /**
   * Action for setting vector search dimensions
   * @param dim dimensions to set
   */

  private case class SetVectorSearchDimensions(private val dim: Int)
    extends SearchFieldAction {
    override def apply(field: SearchField): SearchField = {
      field.setVectorSearchDimensions(dim)
    }
  }

  /**
   * Action for applying many actions at once
   * @param actions actions to apply
   */

  private case class FoldActions(private val actions: Seq[SearchFieldAction])
    extends SearchFieldAction {

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
   * Gets an action for setting an analyzer (for both indexing and searching)
   * @param name analyzer to set
   * @return an action for setting a two-way analyzer
   */

  final def forSettingAnalyzer(name: LexicalAnalyzerName): SearchFieldAction = SetAnalyzer(name)

  /**
   * Gets an action for setting an index analyzer
   * @param name analyzer to set
   * @return an action for setting an index analyzer
   */

  final def forSettingIndexAnalyzer(name: LexicalAnalyzerName): SearchFieldAction = SetIndexAnalyzer(name)

  /**
   * Gets an action for setting a search analyzer
   * @param name analyzer to set
   * @return an action for setting a search analyzer
   */

  final def forSettingSearchAnalyzer(name: LexicalAnalyzerName): SearchFieldAction = SetSearchAnalyzer(name)

  /**
   * Gets an action for setting attribute <code>vectorSearchProfile</code> on a Search field
   * @param profile profile to set
   * @return an action for setting the vector search profile
   * @since 0.10.0
   */

  final def forSettingVectorSearchProfile(profile: String): SearchFieldAction = SetVectorSearchProfile(profile)

  /**
   * Gets an action for setting attribute <code>synonymMapNames</code> on a Search field
   * @param synonyms synonyms to set
   * @return an action for setting the synonyms
   * @since 0.10.2
   */

  final def forSettingSynonyms(synonyms: Seq[String]): SearchFieldAction = SetSynonyms(synonyms)

  /**
   * Gets an action for setting vector search dimensions
   * @param dim dimensions to set
   * @return an action for setting vector search dimensions
   * @since 0.10.2
   */

  final def forSettingVectorSearchDimensions(dim: Int): SearchFieldAction = SetVectorSearchDimensions(dim)

  /**
   * Gets an action for applying many actions at once
   * @param actions actions to apply on a field
   * @return an action for applying many others
   */

  final def forFoldingActions(actions: Seq[SearchFieldAction]): SearchFieldAction = FoldActions(actions)
}
