package io.github.dejarol.azure.search.spark.connector.write.config

import com.azure.search.documents.indexes.models.LexicalAnalyzerName
import io.github.dejarol.azure.search.spark.connector.core.schema.{SearchFieldAction, SearchFieldFeature}
import io.github.dejarol.azure.search.spark.connector.core.utils.json.{Json4SCustomSerializers, JsonBackends}
import org.json4s.{DefaultFormats, Formats}

/**
 * Model for collecting attributes to be set on a [[com.azure.search.documents.indexes.models.SearchField]].
 * When not defined, the default values of Azure AI Search client library will be used
 * @param analyzer analyzer name (for both searching and indexing)
 * @param facetable flag that indicates if the field should be facetable
 * @param filterable flag that indicates if the field should be filterable
 * @param indexAnalyzer analyzer name (for indexing only)
 * @param key flag that indicates if the field should be the index key
 * @param retrievable flag that indicates if the field should be retrievable
 * @param searchAnalyzer analyzer name (for searching only)
 * @param searchable flag that indicates if the field should be searchable
 * @param sortable flag that indicates if the field should be sortable
 * @param vectorSearchProfile name of the vector search profile
 * @param synonymMaps sequence of synonyms
 * @param dimensions vector search dimensions
 * @since 0.10.0
 */

case class SearchFieldMetadata(
                                analyzer: Option[LexicalAnalyzerName],
                                facetable: Option[Boolean],
                                filterable: Option[Boolean],
                                indexAnalyzer: Option[LexicalAnalyzerName],
                                key: Option[Boolean],
                                retrievable: Option[Boolean],
                                searchAnalyzer: Option[LexicalAnalyzerName],
                                searchable: Option[Boolean],
                                sortable: Option[Boolean],
                                vectorSearchProfile: Option[String],
                                synonymMaps: Option[Seq[String]],
                                dimensions: Option[Int]
                              ) {

  /**
   * Return a copy of this instance, but with the key field enabled
   * @return a copy of this instance, but with the key field enabled
   * @since 0.11.0
   */

  def withKeyFieldEnabled: SearchFieldMetadata = copy(key = Some(true))

  /**
   * Gets an action that, if applied, will set all defined options to a field
   * @return an action for applying all defined field options
   */

  def toAction: Option[SearchFieldAction] = {

    // Map each attribute to its related action,
    // then collect all defined actions into a single action

    val definedActions: Seq[SearchFieldAction] = Seq(
      analyzer.map(SearchFieldActions.forSettingAnalyzer),
      facetable.map(SearchFieldActions.forEnablingOrDisablingFeature(SearchFieldFeature.FACETABLE, _)),
      filterable.map(SearchFieldActions.forEnablingOrDisablingFeature(SearchFieldFeature.FILTERABLE, _)),
      indexAnalyzer.map(SearchFieldActions.forSettingIndexAnalyzer),
      key.map(SearchFieldActions.forEnablingOrDisablingFeature(SearchFieldFeature.KEY, _)),
      retrievable.collect {
        case false => SearchFieldActions.forEnablingOrDisablingFeature(
          SearchFieldFeature.HIDDEN, flag = true
        )
      },
      searchAnalyzer.map(SearchFieldActions.forSettingSearchAnalyzer),
      searchable.map(SearchFieldActions.forEnablingOrDisablingFeature(SearchFieldFeature.SEARCHABLE, _)),
      sortable.map(SearchFieldActions.forEnablingOrDisablingFeature(SearchFieldFeature.SORTABLE, _)),
      vectorSearchProfile.map(SearchFieldActions.forSettingVectorSearchProfile),
      synonymMaps.map(SearchFieldActions.forSettingSynonyms),
      dimensions.map(SearchFieldActions.forSettingVectorSearchDimensions)
    ).collect {
      case Some(value) => value
    }

    if (definedActions.nonEmpty) {
      Some(
        SearchFieldActions.forFoldingActions(definedActions)
      )
    } else {
      None
    }
  }
}

object SearchFieldMetadata {

  /**
   * Safely deserialize a JSON string into an instance
   * @param json JSON string to deserialize
   * @return either the exception raised by the deserialization process or an instance
   * @since 0.12.0
   */

  final def safelyFromJsonString(json: String): Either[Throwable, SearchFieldMetadata] = {

    val formats: Formats = DefaultFormats + Json4SCustomSerializers.forLexicalAnalyzerName
    JsonBackends.json4s[SearchFieldMetadata](formats).safelyDeserialize(json)
  }

  /**
   * Create an empty instance (i.e. an instance with no option defined)
   * @return an empty instance
   * @since 0.11.0
   */

  final def empty(): SearchFieldMetadata = {

    SearchFieldMetadata(
      analyzer = None,
      facetable = None,
      filterable = None,
      indexAnalyzer = None,
      key = None,
      retrievable = None,
      searchAnalyzer = None,
      searchable = None,
      sortable = None,
      vectorSearchProfile = None,
      synonymMaps = None,
      dimensions = None
    )
  }
}