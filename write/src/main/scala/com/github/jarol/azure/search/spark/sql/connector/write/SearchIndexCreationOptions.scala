package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.json.JsonReader
import com.azure.search.documents.indexes.models.{CharFilter, LexicalAnalyzer, LexicalTokenizer, SearchSuggester, SimilarityAlgorithm}
import com.github.jarol.azure.search.spark.sql.connector.core.config.SearchConfig
import com.github.jarol.azure.search.spark.sql.connector.core.utils.Json
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * Collector of options to apply at index creation time
 * @param options options for index creation
 */

case class SearchIndexCreationOptions(override protected val options: CaseInsensitiveMap[String])
  extends SearchConfig(options) {

  /**
   * Get an optional instance representing an Azure Search API model
   * (like [[SimilarityAlgorithm]], [[LexicalTokenizer]], etc ...)
   * from the json string related to a configuration key
   * @param key configuration key
   * @param function deserialization function
   * @tparam T target type
   * @return an optional instance of target type
   */

  private def getAzModel[T](
                             key: String,
                             function: JsonReader => T
                           ): Option[T] = {

    getAs[T](
      key,
      jsonString => Json.unsafelyReadAzModel[T](
        jsonString,
        function
      )
    )
  }

  /**
   * Get an optional collection of instances representing Azure Search API models
   * (like [[SimilarityAlgorithm]], [[LexicalTokenizer]], etc ...)
   * from the json string related to a configuration key
   * @param key configuration key
   * @param function deserialization function
   * @tparam T target type
   * @return an optional collection of Azure API models
   */

  private def getArrayOfAzModels[T](
                                     key: String,
                                     function: JsonReader => T
                                   ): Option[Seq[T]] = {

    getAsListOf[T](
      key,
      jsonString => Json.unsafelyReadAzModelArray[T](
        jsonString,
        function
      )
    )
  }

  /**
   * Get the (optional) similarity algorithm to set on the newly created Azure Search index
   * @return the [[SimilarityAlgorithm]] for the new index
   */

  private[write] def similarityAlgorithm: Option[SimilarityAlgorithm] = {

    getAzModel[SimilarityAlgorithm](
      WriteConfig.SIMILARITY_CONFIG,
      SimilarityAlgorithm.fromJson
    )
  }

  /**
   * Get the (optional) collection of tokenizers to set on the newly created Azure Search index
   * @return the collection of [[LexicalTokenizer]] for the new index
   */

  private[write] def tokenizers: Option[Seq[LexicalTokenizer]] = {

    getArrayOfAzModels[LexicalTokenizer](
      WriteConfig.TOKENIZERS_CONFIG,
      LexicalTokenizer.fromJson
    )
  }

  /**
   * Get the (optional) collection of search suggesters to set on the newly created Azure Search index
   * @return the collection of [[SearchSuggester]] for the new index
   */

  private[write] def searchSuggesters: Option[Seq[SearchSuggester]] = {

    getArrayOfAzModels[SearchSuggester](
      WriteConfig.SEARCH_SUGGESTERS_CONFIG,
      SearchSuggester.fromJson
    )
  }

  /**
   * Get the (optional) collection of [[LexicalAnalyzer]] to set on newly created index
   * @return collection of [[LexicalAnalyzer]] for the new index
   */

  private[write] def analyzers: Option[Seq[LexicalAnalyzer]] = {

    getArrayOfAzModels[LexicalAnalyzer](
      WriteConfig.ANALYZERS_CONFIG,
      LexicalAnalyzer.fromJson
    )
  }

  /**
   * Get the (optional) collection of [[CharFilter]] to set on newly created index
   * @return collection of [[CharFilter]] for the new index
   */

  private[write] def charFilters: Option[Seq[CharFilter]] = {

    getArrayOfAzModels[CharFilter](
      WriteConfig.CHAR_FILTERS_CONFIG,
      CharFilter.fromJson
    )
  }

  /**
   * Get the set of actions to apply on a simple Search index
   * @return actions to apply in order to enrich a Search index definition
   */

  final def searchIndexActions: Seq[SearchIndexAction] = {

    Seq(
      similarityAlgorithm.map(SearchIndexActions.forSettingSimilarityAlgorithm),
      tokenizers.map(SearchIndexActions.forSettingTokenizers),
      searchSuggesters.map(SearchIndexActions.forSettingSuggesters),
      analyzers.map(SearchIndexActions.forSettingAnalyzers),
      charFilters.map(SearchIndexActions.forSettingCharFilters)
    ).collect {
      case Some(value) => value
    }
  }
}

object SearchIndexCreationOptions {

  /**
   * Create an instance from a [[SearchConfig]]
   * @param config configuration object
   * @return an instance of [[SearchIndexCreationOptions]]
   */

  def apply(config: SearchConfig): SearchIndexCreationOptions = {

    SearchIndexCreationOptions(
      CaseInsensitiveMap[String](
        config.toMap
      )
    )
  }
}
