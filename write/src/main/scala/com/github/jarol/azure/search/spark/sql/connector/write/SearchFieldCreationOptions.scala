package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.core.config.SearchConfig
import com.github.jarol.azure.search.spark.sql.connector.core.schema.{SchemaUtils, SearchFieldAction, SearchFieldFeature}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types.StructField

/**
 * Options for enriching a field of a Search index
 * <br>
 * Among field enrichment options, there are
 *  - setting the value for its boolean properties (filterable, sortable, etc ...)
 *  - setting some analyzers
 *
 * <br>
 * Field names should be referenced in a path-like fashion, i.e.
 *  - a top-level field should be referenced simply by name
 *  - a nested field should be referenced using the notation <b>parent.child</b>
 * @param options options for field enrichment
 * @param indexActionColumn column name for retrieving a per-document [[com.azure.search.documents.models.IndexActionType]]
 */

case class SearchFieldCreationOptions(
                                       override protected val options: CaseInsensitiveMap[String],
                                       indexActionColumn: Option[String]
                                     )
  extends SearchConfig(options) {

  /**
   * Get the name of the key field
   * @return the key field name
   */

  private[write] def keyField: String = unsafelyGet(WriteConfig.KEY_FIELD_CONFIG, Some(WriteConfig.FIELD_OPTIONS_PREFIX), None)

  /**
   * Get the name of fields that should be disabled from filtering
   * @return field for which the <b>filterable</b> property will be set to false
   */

  private[write] def disabledFromFiltering: Option[Seq[String]] = getAsList(WriteConfig.DISABLE_FILTERING_CONFIG)

  /**
   * Get the name of fields that should be disabled from sorting
   * @return field for which the <b>sortable</b> property should be set to false
   */

  private[write] def disabledFromSorting: Option[Seq[String]] = getAsList(WriteConfig.DISABLE_SORTING_CONFIG)

  /**
   * Get the name of fields that should be hidden
   * @return field for which the <b>hidden</b> property should be set to true
   */

  private[write] def hiddenFields: Option[Seq[String]] = getAsList(WriteConfig.HIDDEN_FIELDS_CONFIG)

  /**
   * Get the name of fields that should be disabled from searching
   * @return field for which the <b>searchable</b> property should be set to false
   */

  private[write] def disabledFromSearch: Option[Seq[String]] = getAsList(WriteConfig.DISABLE_SEARCH_CONFIG)

  /**
   * Get the name of fields that should be disabled from faceting
   * @return field for which the <b>facetable</b> property should be set to false
   */

  private[write] def disabledFromFaceting: Option[Seq[String]] = getAsList(WriteConfig.DISABLE_FACETING_CONFIG)

  /**
   * If defined, remove the index action column field from a schema
   * @param schema Spark schema
   * @return input schema filtered in order to exclude index action field
   */

  final def excludeIndexActionColumn(schema: Seq[StructField]): Seq[StructField] = {

    // If an index action column is defined, it should be excluded from the schema
    indexActionColumn match {
      case Some(value) => schema.filterNot {
        _.name.equalsIgnoreCase(value)
      }
      case None => schema
    }
  }

  /**
   * Get the actions to apply on [[SearchField]](s) in order to enable/disable their features
   * @return actions for enabling/disabling features
   */

  private def actionsForFeatures: Seq[(String, SearchFieldAction)] = {

    Map(
      SearchFieldActions.forEnablingFeature(SearchFieldFeature.KEY) -> Some(Seq(keyField)),
      SearchFieldActions.forDisablingFeature(SearchFieldFeature.FILTERABLE) -> disabledFromFiltering,
      SearchFieldActions.forDisablingFeature(SearchFieldFeature.SORTABLE) -> disabledFromSorting,
      SearchFieldActions.forEnablingFeature(SearchFieldFeature.HIDDEN) -> hiddenFields,
      SearchFieldActions.forDisablingFeature(SearchFieldFeature.SEARCHABLE) -> disabledFromSearch,
      SearchFieldActions.forDisablingFeature(SearchFieldFeature.FACETABLE) -> disabledFromFaceting
    ).collect {
      case (action, Some(values)) => values.map {
        s => (s, action)
      }
    }.flatten.toSeq
  }

  private[write] def analyzerConfigs: Option[Seq[AnalyzerConfig]] = {

    getAsListOf[AnalyzerConfig](
      WriteConfig.ANALYZERS_CONFIG,
      _ => Seq.empty[AnalyzerConfig]
    )
  }

  /**
   * Get the actions to apply on [[SearchField]](s) in order to set analyzers
   * @return actions for setting analyzers
   */

  private def actionsForAnalyzers: Seq[(String, SearchFieldAction)] = {

    analyzerConfigs
      .map(_.flatMap(_.actions))
      .getOrElse(Seq.empty)
  }

  /**
   * Create a map that collects the set of actions to apply for each field.
   * Keys will be field paths (i.e. <code>name</code> for a top-level atomic field,
   * or <code>address.city</code> for a nested field)
   * @return a map with keys begin field paths and values being the actions to apply on such field
   */

  private[write] def getActionsMap: Map[String, Seq[SearchFieldAction]] = {

    // Group all actions related to same field
    (actionsForFeatures ++ actionsForAnalyzers).groupBy {
      case (str, _) => str
    }.mapValues {
      _.map {
        case (_, action) => action
      }
    }
  }

  /**
   * Starting from a Spark schema, create a collection of [[SearchField]](s)
   * @param schema Dataframe schema
   * @return a collection of Search fields for index creation
   */

  def toSearchFields(schema: Seq[StructField]): Seq[SearchField] = {

    val actions = getActionsMap
    excludeIndexActionColumn(schema).map {
      structField =>
        SchemaUtils.toSearchField(structField, actions, None)
    }
  }
}

object SearchFieldCreationOptions {

  // TODO: add doc

  def apply(
             config: SearchConfig,
             indexActionColumn: Option[String]
           ): SearchFieldCreationOptions = {

    SearchFieldCreationOptions(
      CaseInsensitiveMap(config.toMap),
      indexActionColumn
    )
  }
}