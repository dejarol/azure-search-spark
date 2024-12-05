package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.{LexicalTokenizer, SearchIndex, SearchSuggester}

/**
 * Collection of factory methods for creating [[SearchIndexAction]]
 */

object SearchIndexActions {

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
