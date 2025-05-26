package io.github.dejarol.azure.search.spark.connector.write.config

import com.azure.json.JsonReader
import com.azure.search.documents.indexes.models._
import io.github.dejarol.azure.search.spark.connector.core.config.SearchConfig
import io.github.dejarol.azure.search.spark.connector.core.utils.json.Json
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * Collector of options to apply at index creation time
 * @param options options for index creation
 */

case class SearchIndexEnrichmentOptions(override protected val options: CaseInsensitiveMap[String])
  extends SearchConfig(options) {

  /**
   * Gets an optional instance representing an Azure Search API model
   * (like [[SimilarityAlgorithm]], [[LexicalTokenizer]], etc ...)
   * from the JSON string related to a configuration key
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
   * Gets an optional collection of instances representing Azure Search API models
   * (like [[SimilarityAlgorithm]], [[LexicalTokenizer]], etc ...)
   * from the JSON string related to a configuration key
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
   * Gets the (optional) similarity algorithm to set on index definition
   * @return the [[SimilarityAlgorithm]] for the new index
   */

  private[config] def similarityAlgorithm: Option[SimilarityAlgorithm] = {

    getAzModel[SimilarityAlgorithm](
      SearchIndexEnrichmentOptions.SIMILARITY_CONFIG,
      SimilarityAlgorithm.fromJson
    )
  }

  /**
   * Gets the (optional) collection of tokenizers to set on index definition
   * @return the collection of [[LexicalTokenizer]] for the new index
   */

  private[config] def tokenizers: Option[Seq[LexicalTokenizer]] = {

    getArrayOfAzModels[LexicalTokenizer](
      SearchIndexEnrichmentOptions.TOKENIZERS_CONFIG,
      LexicalTokenizer.fromJson
    )
  }

  /**
   * Gets the (optional) collection of search suggesters to set on index definition
   * @return the collection of [[SearchSuggester]] for the new index
   */

  private[config] def searchSuggesters: Option[Seq[SearchSuggester]] = {

    getArrayOfAzModels[SearchSuggester](
      SearchIndexEnrichmentOptions.SUGGESTERS_CONFIG,
      SearchSuggester.fromJson
    )
  }

  /**
   * Gets the (optional) collection of [[LexicalAnalyzer]] to set on index definition
   * @return collection of [[LexicalAnalyzer]] for the new index
   */

  private[config] def analyzers: Option[Seq[LexicalAnalyzer]] = {

    getArrayOfAzModels[LexicalAnalyzer](
      SearchIndexEnrichmentOptions.ANALYZERS_CONFIG,
      LexicalAnalyzer.fromJson
    )
  }

  /**
   * Gets the (optional) collection of [[CharFilter]] to set on index definition
   * @return collection of [[CharFilter]] for the new index
   */

  private[config] def charFilters: Option[Seq[CharFilter]] = {

    getArrayOfAzModels[CharFilter](
      SearchIndexEnrichmentOptions.CHAR_FILTERS_CONFIG,
      CharFilter.fromJson
    )
  }

  /**
   * Gets the (optional) collection of [[ScoringProfile]] to set on index definition
   * @return collection of [[ScoringProfile]] for the new index
   */

  private[config] def scoringProfiles: Option[Seq[ScoringProfile]] = {

    getArrayOfAzModels[ScoringProfile](
      SearchIndexEnrichmentOptions.SCORING_PROFILES_CONFIG,
      ScoringProfile.fromJson
    )
  }

  /**
   * Gets the token filters to set on index definition
   * @return the (optional) collection of token filters
   */

  private[config] def tokenFilters: Option[Seq[TokenFilter]] = {

    getArrayOfAzModels[TokenFilter](
      SearchIndexEnrichmentOptions.TOKEN_FILTERS_CONFIG,
      TokenFilter.fromJson
    )
  }

  /**
   * Gets the CORS options to set on index definition
   * @return the (optional) CORS options
   */

  private[config] def corsOptions: Option[CorsOptions] = {

    getAzModel[CorsOptions](
      SearchIndexEnrichmentOptions.CORS_OPTIONS_CONFIG,
      CorsOptions.fromJson
    )
  }

  /**
   * Gets the name of the default scoring profile to set on index definition
   * @return name of default scoring profile
   */

  private[config] def defaultScoringProfile: Option[String] = {

    get(SearchIndexEnrichmentOptions.DEFAULT_SCORING_PROFILE_CONFIG)
  }

  /**
   * Gets the vector search configuration to set on index definition
   * @return the (optional) vector search configuration
   * @since 0.10.0
   */

  private[config] def vectorSearch: Option[VectorSearch] = {

    getAzModel[VectorSearch](
      SearchIndexEnrichmentOptions.VECTOR_SEARCH_CONFIG,
      VectorSearch.fromJson
    )
  }

  /**
   * Gets the semantic search configuration to set on index definition
   * @return the (optional) semantic search configuration
   * @since 0.10.1
   */

  private[config] def semanticSearch: Option[SemanticSearch] = {

    getAzModel[SemanticSearch](
      SearchIndexEnrichmentOptions.SEMANTIC_SEARCH_CONFIG,
      SemanticSearch.fromJson
    )
  }

  /**
   * Gets the encryption key to set on index definition
   * @return the (optional) encryption key
   * @since 0.10.1
   */

  private[config] def encryptionKey: Option[SearchResourceEncryptionKey] = {

    getAzModel[SearchResourceEncryptionKey](
      SearchIndexEnrichmentOptions.ENCRYPTION_KEY_CONFIG,
      SearchResourceEncryptionKey.fromJson
    )
  }

  /**
   * Gets the etag to set on index definition
   * @return the (optional) etag
   * @since 0.10.2
   */

  private[config] def eTag: Option[String] = get(SearchIndexEnrichmentOptions.ETAG_CONFIG)

  /**
   * Gets an optional action to apply on a Search index.
   * If any of the inner actions is defined (i.e. setting
   * <code>similarityAlgorithm</code>, <code>tokenizers</code>, etc ....),
   * an action is returned
   * @return an optional action
   */

  final def action: Option[SearchIndexAction] = {

    // Collect only defined actions
    val definedActions = Seq(
      similarityAlgorithm.map(SearchIndexActions.forSettingSimilarityAlgorithm),
      tokenizers.map(SearchIndexActions.forSettingTokenizers),
      searchSuggesters.map(SearchIndexActions.forSettingSuggesters),
      analyzers.map(SearchIndexActions.forSettingAnalyzers),
      charFilters.map(SearchIndexActions.forSettingCharFilters),
      scoringProfiles.map(SearchIndexActions.forSettingScoringProfiles),
      tokenFilters.map(SearchIndexActions.forSettingTokenFilters),
      corsOptions.map(SearchIndexActions.forSettingCorsOptions),
      defaultScoringProfile.map(SearchIndexActions.forSettingDefaultScoringProfile),
      vectorSearch.map(SearchIndexActions.forSettingVectorSearch),
      semanticSearch.map(SearchIndexActions.forSettingSemanticSearch),
      encryptionKey.map(SearchIndexActions.forSettingEncryptionKey),
      eTag.map(SearchIndexActions.forSettingETag)
    ).collect {
      case Some(value) => value
    }

    // If any, create a folding action
    if (definedActions.nonEmpty) {
      Some(
        SearchIndexActions.forFoldingActions(definedActions)
      )
    } else {
      None
    }
  }
}

object SearchIndexEnrichmentOptions {

  final val SIMILARITY_CONFIG = "similarity"
  final val TOKENIZERS_CONFIG = "tokenizers"
  final val SUGGESTERS_CONFIG = "suggesters"
  final val ANALYZERS_CONFIG = "analyzers"
  final val CHAR_FILTERS_CONFIG = "charFilters"
  final val SCORING_PROFILES_CONFIG = "scoringProfiles"
  final val TOKEN_FILTERS_CONFIG = "tokenFilters"
  final val CORS_OPTIONS_CONFIG = "corsOptions"
  final val DEFAULT_SCORING_PROFILE_CONFIG = "defaultScoringProfile"
  final val VECTOR_SEARCH_CONFIG = "vectorSearch"
  final val SEMANTIC_SEARCH_CONFIG = "semanticSearch"
  final val ENCRYPTION_KEY_CONFIG = "encryptionKey"
  final val ETAG_CONFIG = "etag"

  /**
   * Creates an instance from a [[io.github.dejarol.azure.search.spark.connector.core.config.SearchConfig]]
   *
   * @param config configuration object
   * @return an instance of [[SearchIndexEnrichmentOptions]]
   */

  def apply(config: SearchConfig): SearchIndexEnrichmentOptions = {

    SearchIndexEnrichmentOptions(
      CaseInsensitiveMap[String](
        config.toMap
      )
    )
  }

}
