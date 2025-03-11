package io.github.dejarol.azure.search.spark.connector.write.config

import com.azure.json.JsonReader
import com.azure.search.documents.indexes.models._
import io.github.dejarol.azure.search.spark.connector.write.{SearchIndexAction, SearchIndexActions}
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
   * Get an optional collection of instances representing Azure Search API models
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
   * Get the (optional) similarity algorithm to set on index definition
   * @return the [[SimilarityAlgorithm]] for the new index
   */

  private[write] def similarityAlgorithm: Option[SimilarityAlgorithm] = {

    getAzModel[SimilarityAlgorithm](
      SearchIndexCreationOptions.SIMILARITY_CONFIG,
      SimilarityAlgorithm.fromJson
    )
  }

  /**
   * Get the (optional) collection of tokenizers to set on index definition
   * @return the collection of [[LexicalTokenizer]] for the new index
   */

  private[write] def tokenizers: Option[Seq[LexicalTokenizer]] = {

    getArrayOfAzModels[LexicalTokenizer](
      SearchIndexCreationOptions.TOKENIZERS_CONFIG,
      LexicalTokenizer.fromJson
    )
  }

  /**
   * Get the (optional) collection of search suggesters to set on index definition
   * @return the collection of [[SearchSuggester]] for the new index
   */

  private[write] def searchSuggesters: Option[Seq[SearchSuggester]] = {

    getArrayOfAzModels[SearchSuggester](
      SearchIndexCreationOptions.SUGGESTERS_CONFIG,
      SearchSuggester.fromJson
    )
  }

  /**
   * Get the (optional) collection of [[LexicalAnalyzer]] to set on index definition
   * @return collection of [[LexicalAnalyzer]] for the new index
   */

  private[write] def analyzers: Option[Seq[LexicalAnalyzer]] = {

    getArrayOfAzModels[LexicalAnalyzer](
      SearchIndexCreationOptions.ANALYZERS_CONFIG,
      LexicalAnalyzer.fromJson
    )
  }

  /**
   * Get the (optional) collection of [[CharFilter]] to set on index definition
   * @return collection of [[CharFilter]] for the new index
   */

  private[write] def charFilters: Option[Seq[CharFilter]] = {

    getArrayOfAzModels[CharFilter](
      SearchIndexCreationOptions.CHAR_FILTERS_CONFIG,
      CharFilter.fromJson
    )
  }

  /**
   * Get the (optional) collection of [[ScoringProfile]] to set on index definition
   * @return collection of [[ScoringProfile]] for the new index
   */

  private[write] def scoringProfiles: Option[Seq[ScoringProfile]] = {

    getArrayOfAzModels[ScoringProfile](
      SearchIndexCreationOptions.SCORING_PROFILES_CONFIG,
      ScoringProfile.fromJson
    )
  }

  /**
   * Get the token filters to set on index definition
   * @return the (optional) collection of token filters
   */
    
  private[write] def tokenFilters: Option[Seq[TokenFilter]] = {
    
    getArrayOfAzModels[TokenFilter](
      SearchIndexCreationOptions.TOKEN_FILTERS_CONFIG,
      TokenFilter.fromJson
    )
  }

  /**
   * Get the CORS options to set on index definition
   * @return the (optional) CORS options
   */

  private[write] def corsOptions: Option[CorsOptions] = {

    getAzModel[CorsOptions](
      SearchIndexCreationOptions.CORS_OPTIONS_CONFIG,
      CorsOptions.fromJson
    )
  }

  /**
   * Get the name of the default scoring profile to set on index definition
   * @return name of default scoring profile
   */

  private[write] def defaultScoringProfile: Option[String] = get(SearchIndexCreationOptions.DEFAULT_SCORING_PROFILE_CONFIG)

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
      charFilters.map(SearchIndexActions.forSettingCharFilters),
      scoringProfiles.map(SearchIndexActions.forSettingScoringProfiles),
      tokenFilters.map(SearchIndexActions.forSettingTokenFilters),
      corsOptions.map(SearchIndexActions.forSettingCorsOptions),
      defaultScoringProfile.map(SearchIndexActions.forSettingDefaultScoringProfile)
    ).collect {
      case Some(value) => value
    }
  }
}

object SearchIndexCreationOptions {

  final val SIMILARITY_CONFIG = "similarity"
  final val TOKENIZERS_CONFIG = "tokenizers"
  final val SUGGESTERS_CONFIG = "suggesters"
  final val ANALYZERS_CONFIG = "analyzers"
  final val CHAR_FILTERS_CONFIG = "charFilters"
  final val SCORING_PROFILES_CONFIG = "scoringProfiles"
  final val TOKEN_FILTERS_CONFIG = "tokenFilters"
  final val CORS_OPTIONS_CONFIG = "corsOptions"
  final val DEFAULT_SCORING_PROFILE_CONFIG = "defaultScoringProfile"

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
