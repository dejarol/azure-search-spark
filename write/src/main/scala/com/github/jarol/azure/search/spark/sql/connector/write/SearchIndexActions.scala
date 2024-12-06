package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.{LexicalTokenizer, SearchIndex, SearchSuggester, SimilarityAlgorithm}

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
   * Create an action for setting the similarity algorithm
   * @param algorithm algorithm to set
   * @return an action for setting the similarity algorithm
   */

  final def forSettingSimilarityAlgorithm(algorithm: SimilarityAlgorithm): SearchIndexAction = SetSimilarityAlgorithm(algorithm)

  /**
   * Create an action for setting some tokenizers on a [[SearchIndex]]
   * @param tokenizers tokenizer to set
   * @return an action for setting some tokenizers on a [[SearchIndex]]
   */

  final def forSettingTokenizers(tokenizers: Seq[LexicalTokenizer]): SearchIndexAction = SetTokenizers(tokenizers)

  /**
   * Create an action for setting suggesters
   * @param suggesters suggesters to set
   * @return an action for setting suggesters
   */

  final def forSettingSuggesters(suggesters: Seq[SearchSuggester]): SearchIndexAction = SetSuggesters(suggesters)
}
