package com.github.jarol.azure.search.spark.sql.connector.write

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
   * Create an action for setting the similarity algorithm
   * @param algorithm algorithm to set
   * @return an action for setting the similarity algorithm
   */

  final def forSettingSimilarityAlgorithm(algorithm: SimilarityAlgorithm): SearchIndexAction = SetSimilarityAlgorithm(algorithm)

  /**
   * Create an action for setting some tokenizers
   * @param tokenizers tokenizer to set
   * @return an action for setting some tokenizers
   */

  final def forSettingTokenizers(tokenizers: Seq[LexicalTokenizer]): SearchIndexAction = SetTokenizers(tokenizers)

  /**
   * Create an action for setting suggesters
   * @param suggesters suggesters to set
   * @return an action for setting suggesters
   */

  final def forSettingSuggesters(suggesters: Seq[SearchSuggester]): SearchIndexAction = SetSuggesters(suggesters)

  /**
   * Create an action for setting analyzers
   * @param analyzers analyzers to set
   * @return an action for setting some lexical analyzers
   */

  final def forSettingAnalyzers(analyzers: Seq[LexicalAnalyzer]): SearchIndexAction = SetAnalyzers(analyzers)

  /**
   * Create an action for setting char filters
   * @param charFilters filters to set
   * @return an action for setting some char filters
   */

  final def forSettingCharFilters(charFilters: Seq[CharFilter]): SearchIndexAction = SetCharFilters(charFilters)

  /**
   * Create an action for setting the scoring profiles
   * @param profiles profiles to add
   * @return an action for setting the scoring profiles
   */

  final def forSettingScoringProfiles(profiles: Seq[ScoringProfile]): SearchIndexAction = SetScoringProfiles(profiles)

  /**
   * Create an action for setting the token filters
   * @param tokenFilters token filters
   * @return an action for setting token filters
   */

  final def forSettingTokenFilters(tokenFilters: Seq[TokenFilter]): SearchIndexAction = SetTokenFilters(tokenFilters)

  /**
   * Create an action for setting CORS options
   * @param corsOptions CORS options to set
   * @return an action for setting CORS options
   */

  final def forSettingCorsOptions(corsOptions: CorsOptions): SearchIndexAction = SetCorsOptions(corsOptions)
}
