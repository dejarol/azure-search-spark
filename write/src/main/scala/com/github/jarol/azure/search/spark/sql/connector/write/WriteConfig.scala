package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.json.JsonReader
import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.{CharFilter, IndexDocumentsBatch, LexicalAnalyzer, LexicalTokenizer, SearchSuggester, SimilarityAlgorithm}
import com.azure.search.documents.models.IndexActionType
import com.github.jarol.azure.search.spark.sql.connector.core.config.SearchIOConfig
import com.github.jarol.azure.search.spark.sql.connector.core.utils.{Enums, Json}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * Write configuration
 * @param options  write options passed to the dataSource
 */

case class WriteConfig(override protected val options: CaseInsensitiveMap[String])
  extends SearchIOConfig(options) {

  /**
   * Index a batch of documents on target index
   * @param documents documents to index
   */

  final def indexDocuments(documents: IndexDocumentsBatch[SearchDocument]): Unit = {

    withSearchClientDo {
      _.indexDocuments(documents)
    }
  }

  /**
   * Get the batch size to be used for writing documents along partitions
   * @return batch size for writing
   */

  final def batchSize: Int = {

    getOrDefaultAs(
      WriteConfig.BATCH_SIZE_CONFIG,
      WriteConfig.DEFAULT_BATCH_SIZE_VALUE,
      Integer.parseInt
    )
  }

  /**
   * Return the (optional) [[IndexActionType]] to use for indexing all documents
   * @return action type for indexing all documents
   */

  final def maybeUserSpecifiedAction: Option[IndexActionType] = {

    getAs(
      WriteConfig.ACTION_CONFIG,
      WriteConfig.valueOfIndexActionType
    )
  }

  /**
   * Return the [[IndexActionType]] defined by the user or a default. It will be used for indexing all documents.
   * If not specified, [[WriteConfig.DEFAULT_ACTION_TYPE]] will be used
   * @return action type for indexing all documents
   */

  final def overallAction: IndexActionType = maybeUserSpecifiedAction.getOrElse(WriteConfig.DEFAULT_ACTION_TYPE)

  /**
   * Return the name of a dataframe column that contains a per-document action type.
   * It must be the name of an existing string column whose values can be mapped to an [[IndexActionType]]
   * @return column name for document action
   */

  final def actionColumn: Option[String] = get(WriteConfig.INDEX_ACTION_COLUMN_CONFIG)

  /**
   * Get the set of options for defining Search fields for target index
   * @return options for defining fields on target Search index
   */

  final def searchFieldCreationOptions: SearchFieldCreationOptions = {

    SearchFieldCreationOptions(
      getAllWithPrefix(WriteConfig.FIELD_OPTIONS_PREFIX),
      actionColumn
    )
  }

  /**
 * Retrieves the options for creating a search index (i.e. all configurations that have the prefix defined by
 * [[WriteConfig.INDEX_OPTIONS_PREFIX]])
 * @return index creation options
 */

def searchIndexCreationOptions: SearchIndexCreationOptions = {

  SearchIndexCreationOptions(
    getAllWithPrefix(WriteConfig.INDEX_OPTIONS_PREFIX)
  )
}

  /**
   * Get an optional collection of instances representing Azure Search REST API models
   * (like [[SimilarityAlgorithm]], [[LexicalTokenizer]], etc ...) from the json string
   * related to a configuration key
   * @param key configuration key
   * @param function deserialization function
   * @tparam T target type
   * @return an optional collection of Azure API models
   */

  private def getArrayOfAzModels[T](
                                     key: String,
                                     function: JsonReader => T
                                   ): Option[Seq[T]] = {

    getAs[Seq[T]](
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

    getAs[SimilarityAlgorithm](
      WriteConfig.SIMILARITY_CONFIG,
      Json.unsafelyReadAzModel[SimilarityAlgorithm](
        _,
        SimilarityAlgorithm.fromJson
      )
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

object WriteConfig {

  final val BATCH_SIZE_CONFIG = "batchSize"
  final val DEFAULT_BATCH_SIZE_VALUE = 1000
  final val ACTION_CONFIG = "action"
  final val INDEX_ACTION_COLUMN_CONFIG = "actionColumn"
  final val DEFAULT_ACTION_TYPE: IndexActionType = IndexActionType.MERGE_OR_UPLOAD

  final val FIELD_OPTIONS_PREFIX = "fieldOptions."
  final val KEY_FIELD_CONFIG = "key"
  final val DISABLE_FILTERING_CONFIG = "disableFilteringOn"
  final val DISABLE_SORTING_CONFIG = "disableSortingOn"
  final val HIDDEN_FIELDS_CONFIG = "hiddenFields"
  final val DISABLE_SEARCH_CONFIG = "disableSearchOn"
  final val DISABLE_FACETING_CONFIG = "disableFacetingOn"

  final val INDEX_OPTIONS_PREFIX = "indexOptions."

  final val ANALYZERS_PREFIX = "analyzers."
  final val ALIASES_SUFFIX = "aliases"
  final val NAME_SUFFIX = "name"
  final val TYPE_SUFFIX = "type"
  final val ON_FIELDS_SUFFIX = "onFields"

  final val SIMILARITY_CONFIG = "similarity"
  final val TOKENIZERS_CONFIG = "tokenizers"
  final val SEARCH_SUGGESTERS_CONFIG = "searchSuggesters"
  final val ANALYZERS_CONFIG = "analyzers"
  final val CHAR_FILTERS_CONFIG = "charFilters"

  /**
   * Create an instance from a simple map
   * @param options local options
   * @return a write config
   */

  def apply(options: Map[String, String]): WriteConfig = {

    WriteConfig(
      CaseInsensitiveMap(options)
    )
  }

  /**
   * Retrieve the action type related to a value
   * @param value string value
   * @return the [[IndexActionType]] with same case-insensitive name or inner value
   */

  private[write] def valueOfIndexActionType(value: String): IndexActionType = {

    Enums.unsafeValueOf[IndexActionType](
      value,
      (v, s) => v.name().equalsIgnoreCase(s) ||
        v.toString.equalsIgnoreCase(s)
    )
  }
}