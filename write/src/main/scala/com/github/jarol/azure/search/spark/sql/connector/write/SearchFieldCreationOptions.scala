package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.core.schema.{SchemaUtils, SearchFieldAction, SearchFieldFeature}
import org.apache.spark.sql.types.StructField

/**
 * Options for creating a Search index
 * @param keyField field to use as document key
 * @param disabledFromFiltering list of fields that should be disabled from filtering
 * @param disabledFromSorting list of fields that should be disabled from sorting
 * @param hiddenFields list of fields that should be hidden (i.e. non-retrievable)
 * @param disabledFromSearch list of fields that should not be searchable
 * @param disabledFromFaceting list of fields that should not be facetable
 * @param analyzerConfigs list of configuration for setting field analyzers
 * @param indexActionColumn column name for retrieving a per-document [[com.azure.search.documents.models.IndexActionType]]
 */

case class SearchFieldCreationOptions(
                                       keyField: String,
                                       disabledFromFiltering: Option[Seq[String]],
                                       disabledFromSorting: Option[Seq[String]],
                                       hiddenFields: Option[Seq[String]],
                                       disabledFromSearch: Option[Seq[String]],
                                       disabledFromFaceting: Option[Seq[String]],
                                       analyzerConfigs: Option[Seq[AnalyzerConfig]],
                                       indexActionColumn: Option[String]
                                     ) {

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
