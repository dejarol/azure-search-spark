package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.IndexDocumentsBatch
import com.azure.search.documents.models.IndexActionType
import com.github.jarol.azure.search.spark.sql.connector.core.config.SearchIOConfig
import com.github.jarol.azure.search.spark.sql.connector.core.utils.Enums
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * Write configuration
 * @param options  write options passed to the dataSource
 */

case class WriteConfig(override protected val options: CaseInsensitiveMap[String])
  extends SearchIOConfig(options) {

  /**
   * Index a batch of documents on target index
   *
   * @param documents documents to index
   */

  final def indexDocuments(documents: IndexDocumentsBatch[SearchDocument]): Unit = {

    withSearchClientDo {
      _.indexDocuments(documents)
    }
  }

  /**
   * Get the batch size to be used for writing documents along partitions
   *
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
   *
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
   *
   * @return action type for indexing all documents
   */

  final def overallAction: IndexActionType = maybeUserSpecifiedAction.getOrElse(WriteConfig.DEFAULT_ACTION_TYPE)

  /**
   * Return the name of a dataframe column that contains a per-document action type.
   * It must be the name of an existing string column whose values can be mapped to an [[IndexActionType]]
   *
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
   *
   * @return index creation options
   */

  def searchIndexCreationOptions: SearchIndexCreationOptions = {

    SearchIndexCreationOptions(
      getAllWithPrefix(WriteConfig.INDEX_OPTIONS_PREFIX)
    )
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
  final val SIMILARITY_CONFIG = "similarity"
  final val TOKENIZERS_CONFIG = "tokenizers"
  final val SEARCH_SUGGESTERS_CONFIG = "searchSuggesters"
  final val ANALYZERS_CONFIG = "analyzers"
  final val CHAR_FILTERS_CONFIG = "charFilters"
  final val SCORING_PROFILES_CONFIG = "scoringProfiles"
  final val TOKEN_FILTERS_CONFIG = "tokenFilters"

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