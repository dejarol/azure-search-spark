package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.core.schema.{SchemaUtils, SearchFieldAction, SearchFieldActions, SearchFieldFeature}
import org.apache.spark.sql.types.StructField

/**
 * Options for creating a Search index
 * @param keyField field to use as document key
 * @param disabledFromFiltering list of fields that should be disabled from filtering
 * @param disabledFromSorting list of fields that should be disabled from sorting
 * @param hiddenFields list of fields that should be hidden (i.e. non-retrievable)
 * @param disabledFromSearch list of fields that should not be searchable
 * @param disabledFromFaceting list of fields that should not be facetable
 * @param indexActionColumn column name for retrieving a per-document [[com.azure.search.documents.models.IndexActionType]]
 */

case class SearchFieldsCreationOptions(
                                        keyField: String,
                                        disabledFromFiltering: Option[Seq[String]],
                                        disabledFromSorting: Option[Seq[String]],
                                        hiddenFields: Option[Seq[String]],
                                        disabledFromSearch: Option[Seq[String]],
                                        disabledFromFaceting: Option[Seq[String]],
                                        indexActionColumn: Option[String]
                                      ) {

  /**
   * Evaluate if a field should be the key field
   * @param field a struct field
   * @return true for key fields
   */

  final def isKey(field: String): Boolean = field.equalsIgnoreCase(keyField)

  /**
   * Evaluate if a field name exists within an optional list of field names
   * @param field field
   * @param fieldNames optional list of field names
   * @return true for existing fields
   */

  private def fieldNameExistsIn(field: String, fieldNames: Option[Seq[String]]): Boolean = {

    fieldNames.exists {
      _.exists {
        _.equalsIgnoreCase(field)
      }
    }
  }

  /**
   * Evaluate if a field is facetable
   * @param field field
   * @return true for facetable fields
   */

  final def nonFacetable(field: String): Boolean = fieldNameExistsIn(field, disabledFromFaceting)

  /**
   * Evaluate if a fields is filterable
   * @param field field
   * @return true for filterable fields
   */

  final def nonFilterable(field: String): Boolean = fieldNameExistsIn(field, disabledFromFiltering)

  /**
   * Evaluate if a fields is hidden
   * @param field field
   * @return true for hidden fields
   */

  final def isHidden(field: String): Boolean = fieldNameExistsIn(field, hiddenFields)

  /**
   * Evaluate if a fields is searchable
   * @param field field
   * @return true for searchable fields
   */

  final def nonSearchable(field: String): Boolean = fieldNameExistsIn(field, disabledFromSearch)

  /**
   * Evaluate if a fields is sortable
   * @param field field
   * @return true for sortable fields
   */

  final def nonSortable(field: String): Boolean = fieldNameExistsIn(field, disabledFromSorting)

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
   * Create a map that collects the set of actions to apply for each field.
   * Keys will be field paths (i.e. <code>name</code> for a top-level atomic field,
   * or <code>address.city</code> for a nested field)
   * @return a map with keys begin field paths and values being the actions to apply on such field
   */

  private[write] def getActionsMap: Map[String, Seq[SearchFieldAction]] = {

    // Collect defined actions
    val actionTuples: Seq[(String, SearchFieldAction)] = Map(
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

    // Group the actions related to same field
    actionTuples.groupBy {
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

  def schemaToSearchFields(schema: Seq[StructField]): Seq[SearchField] = {

    val actions = getActionsMap
    excludeIndexActionColumn(schema).map {
      structField =>
        SchemaUtils.toSearchField(structField, actions, None)
    }
  }
}
