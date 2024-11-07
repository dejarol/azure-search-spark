package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.core.schema.{SchemaUtils, SearchFieldFeature, toSearchFieldOperations}
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

  final def maybeExcludeIndexActionColumn(schema: Seq[StructField]): Seq[StructField] = {

    // If an index action column is defined, it should be excluded from the schema
    indexActionColumn match {
      case Some(value) => schema.filterNot {
        _.name.equalsIgnoreCase(value)
      }
      case None => schema
    }
  }

  /**
   * Starting from a Spark schema, create a collection of [[SearchField]](s)
   * @param schema Dataframe schema
   * @return a collection of Search fields for index creation
   */

  def schemaToSearchFields(schema: Seq[StructField]): Seq[SearchField] = {

    // For each schema field
    maybeExcludeIndexActionColumn(schema).map {
      structField =>
        val searchField = SchemaUtils.toSearchField(structField)

        // Features to enable (i.e. their flag should be set to true when defined by the user)
        val featuresToEnable: Seq[SearchFieldFeature] = Seq(
          SearchFieldsCreationOptions.featureForPredicate(searchField, isKey, SearchFieldFeature.KEY),
          SearchFieldsCreationOptions.featureForPredicate(searchField, isHidden, SearchFieldFeature.HIDDEN)
        ).collect {
          case Some(value) => value
        }

        // Features to disable (i.e. their flag should be set to false when defined by the user)
        val featuresToDisable: Seq[SearchFieldFeature] = Seq(
          SearchFieldsCreationOptions.featureForPredicate(searchField, nonFacetable, SearchFieldFeature.FACETABLE),
          SearchFieldsCreationOptions.featureForPredicate(searchField, nonFilterable, SearchFieldFeature.FILTERABLE),
          SearchFieldsCreationOptions.featureForPredicate(searchField, nonSearchable, SearchFieldFeature.SEARCHABLE),
          SearchFieldsCreationOptions.featureForPredicate(searchField, nonSortable, SearchFieldFeature.SORTABLE)
        ).collect {
          case Some(value) => value
        }

        searchField
          .enableFeatures(featuresToEnable: _*)
          .disableFeatures(featuresToDisable: _*)
    }
  }
}

object SearchFieldsCreationOptions {

  /**
   * Return an Option wrapping a feature, given that a field name matches a predicate
   * @param field field
   * @param predicate predicate to be matched by field's name
   * @param feature feature to be returned, potentially wrapped by an Option
   * @return a non-empty Option with given feature if the predicate matches
   */

  private def featureForPredicate(
                                   field: SearchField,
                                   predicate: String => Boolean,
                                   feature: SearchFieldFeature
                                 ): Option[SearchFieldFeature] = {

    if (predicate(field.getName)) {
      Some(feature)
    } else {
      None
    }
  }
}
