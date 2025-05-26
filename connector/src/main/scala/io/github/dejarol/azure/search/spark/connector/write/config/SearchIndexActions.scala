package io.github.dejarol.azure.search.spark.connector.write.config

import com.azure.search.documents.indexes.models._

/**
 * Collection of factory methods for creating [[SearchIndexAction]]
 */

object SearchIndexActions {

  /**
   * Action for setting a similarity algorithm
   * @param algorithm algorithm
   */

  private case class SetSimilarityAlgorithm(private val algorithm: SimilarityAlgorithm)
    extends SearchIndexAction {
    override def apply(index: SearchIndex): SearchIndex = {
      index.setSimilarity(algorithm)
    }
  }

  /**
   * Action for setting tokenizers
   * @param tokenizers tokenizers to set
   */

  private case class SetTokenizers(private val tokenizers: Seq[LexicalTokenizer])
    extends SearchIndexAction {
    override def apply(index: SearchIndex): SearchIndex = {
      index.setTokenizers(tokenizers: _*)
    }
  }

  /**
   * Action for setting suggesters
   * @param suggesters suggesters to set
   */

  private case class SetSuggesters(private val suggesters: Seq[SearchSuggester])
    extends SearchIndexAction {
    override def apply(index: SearchIndex): SearchIndex = {
      index.setSuggesters(suggesters: _*)
    }
  }

  /**
   * Action for setting analyzers
   * @param analyzers analyzers to set
   */

  private case class SetAnalyzers(private val analyzers: Seq[LexicalAnalyzer])
    extends SearchIndexAction {
    override def apply(index: SearchIndex): SearchIndex = {
      index.setAnalyzers(analyzers: _*)
    }
  }

  /**
   * Action for setting char filters
   * @param filters filters to set
   */

  private case class SetCharFilters(private val filters: Seq[CharFilter])
    extends SearchIndexAction {
    override def apply(index: SearchIndex): SearchIndex = {
      index.setCharFilters(filters: _*)
    }
  }

  /**
   * Action for setting scoring profiles
   * @param profiles profiles to set
   */

  private case class SetScoringProfiles(private val profiles: Seq[ScoringProfile])
    extends SearchIndexAction {
    override def apply(index: SearchIndex): SearchIndex = {
      index.setScoringProfiles(profiles: _*)
    }
  }

  /**
   * Action for setting token filters
   * @param filters filters to set
   */

  private case class SetTokenFilters(private val filters: Seq[TokenFilter])
    extends SearchIndexAction {
    override def apply(index: SearchIndex): SearchIndex = {
      index.setTokenFilters(filters: _*)
    }
  }

  /**
   * Action for setting CORS options
   * @param corsOptions option to set
   */

  private case class SetCorsOptions(private val corsOptions: CorsOptions)
    extends SearchIndexAction {
    override def apply(index: SearchIndex): SearchIndex = {
      index.setCorsOptions(corsOptions)
    }
  }

  /**
   * Action for setting default scoring profile
   * @param name profile name
   */

  private case class SetDefaultScoringProfile(private val name: String)
    extends SearchIndexAction {
    override def apply(index: SearchIndex): SearchIndex = {
      index.setDefaultScoringProfile(name)
    }
  }

  /**
   * Action for setting the vector search
   * @param vectorSearch vector search
   * @since 0.10.0
   */

  private case class SetVectorSearch(private val vectorSearch: VectorSearch)
    extends SearchIndexAction {
    override def apply(index: SearchIndex): SearchIndex = {
      index.setVectorSearch(vectorSearch)
    }
  }

  /**
   * Action for setting the semantic search
   * @param semanticSearch semantic search
   * @since 0.10.1
   */

  private case class SetSemanticSearch(private val semanticSearch: SemanticSearch)
    extends SearchIndexAction {
    override def apply(index: SearchIndex): SearchIndex = {
      index.setSemanticSearch(semanticSearch)
    }
  }

  /**
   * Action for setting the encryption key
   * @param key key to set
   * @since 0.10.1
   */

  private case class SetEncryptionKey(private val key: SearchResourceEncryptionKey)
    extends SearchIndexAction {
    override def apply(index: SearchIndex): SearchIndex = {
      index.setEncryptionKey(key)
    }
  }

  /**
   * Action for setting the ETag
   * @param tag tag to set
   * @since 0.10.2
   */

  private case class SetETag(private val tag: String)
    extends SearchIndexAction {
    override def apply(index: SearchIndex): SearchIndex = {
      index.setETag(tag)
    }
  }

  /**
   * Action for folding multiple actions
   * @param actions actions to fold
   */

  private case class FoldActions(private val actions: Seq[SearchIndexAction])
    extends SearchIndexAction {

    override def apply(index: SearchIndex): SearchIndex = {

      actions.foldLeft(index) {
        case (index, action) =>
          action.apply(index)
      }
    }
  }

  /**
   * Creates an action for setting the similarity algorithm
   * @param algorithm algorithm to set
   * @return an action for setting the similarity algorithm
   */

  final def forSettingSimilarityAlgorithm(algorithm: SimilarityAlgorithm): SearchIndexAction = SetSimilarityAlgorithm(algorithm)

  /**
   * Creates an action for setting some tokenizers
   * @param tokenizers tokenizer to set
   * @return an action for setting some tokenizers
   */

  final def forSettingTokenizers(tokenizers: Seq[LexicalTokenizer]): SearchIndexAction = SetTokenizers(tokenizers)

  /**
   * Creates an action for setting suggesters
   * @param suggesters suggesters to set
   * @return an action for setting suggesters
   */

  final def forSettingSuggesters(suggesters: Seq[SearchSuggester]): SearchIndexAction = SetSuggesters(suggesters)

  /**
   * Creates an action for setting analyzers
   * @param analyzers analyzers to set
   * @return an action for setting some lexical analyzers
   */

  final def forSettingAnalyzers(analyzers: Seq[LexicalAnalyzer]): SearchIndexAction = SetAnalyzers(analyzers)

  /**
   * Creates an action for setting char filters
   * @param charFilters filters to set
   * @return an action for setting some char filters
   */

  final def forSettingCharFilters(charFilters: Seq[CharFilter]): SearchIndexAction = SetCharFilters(charFilters)

  /**
   * Creates an action for setting the scoring profiles
   * @param profiles profiles to add
   * @return an action for setting the scoring profiles
   */

  final def forSettingScoringProfiles(profiles: Seq[ScoringProfile]): SearchIndexAction = SetScoringProfiles(profiles)

  /**
   * Creates an action for setting the token filters
   * @param tokenFilters token filters
   * @return an action for setting token filters
   */

  final def forSettingTokenFilters(tokenFilters: Seq[TokenFilter]): SearchIndexAction = SetTokenFilters(tokenFilters)

  /**
   * Creates an action for setting CORS options
   * @param corsOptions CORS options to set
   * @return an action for setting CORS options
   */

  final def forSettingCorsOptions(corsOptions: CorsOptions): SearchIndexAction = SetCorsOptions(corsOptions)

  /**
   * Creates an action for setting the default scoring profile
   * @param name profile name
   * @return an action for setting the default scoring profile
   */

  final def forSettingDefaultScoringProfile(name: String): SearchIndexAction = SetDefaultScoringProfile(name)

  /**
   * Creates an action for setting the vector search
   * @param vectorSearch vector search
   * @return an action for setting the vector search
   * @since 0.10.0
   */

  final def forSettingVectorSearch(vectorSearch: VectorSearch): SearchIndexAction = SetVectorSearch(vectorSearch)

  /**
   * Creates an action for setting the semantic search
   * @param semanticSearch semantic search
   * @return an action for setting the semantic search
   * @since 0.10.1
   */

  final def forSettingSemanticSearch(semanticSearch: SemanticSearch): SearchIndexAction = SetSemanticSearch(semanticSearch)

  /**
   * Creates an action for setting the encryption key
   * @param key key
   * @return an action for setting the encryption key
   * @since 0.10.1
   */

  final def forSettingEncryptionKey(key: SearchResourceEncryptionKey): SearchIndexAction = SetEncryptionKey(key)

  /**
   * Creates an action for setting the ETag
   * @param tag ETag
   * @return an action for setting the ETag
   * @since 0.10.2
   */

  final def forSettingETag(tag: String): SearchIndexAction = SetETag(tag)

  /**
   * Creates an action for folding multiple actions
   * @param actions actions to fold
   * @return an action for folding multiple actions
   */

  final def forFoldingActions(actions: Seq[SearchIndexAction]): SearchIndexAction = FoldActions(actions)
}
