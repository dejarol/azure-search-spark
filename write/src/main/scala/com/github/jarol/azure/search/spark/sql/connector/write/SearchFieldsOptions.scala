package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.core.schema.{SchemaUtils, SearchFieldFeature, toSearchFieldOperations}
import org.apache.spark.sql.types.StructField

/**
 * Options for creating a Search index
 * @param keyField field to be used as document key
 * @param filterableFields list of fields that should be filterable
 * @param sortableFields list of fields that should be sortable
 * @param hiddenFields list of fields that should be hidden (i.e. non-retrievable)
 * @param searchableFields list of fields that should be searchable
 * @param facetableFields list of fields that should be facetable
 * @param indexActionColumn column name for retrieving a per-document [[com.azure.search.documents.models.IndexActionType]]
 */

case class SearchFieldsOptions(
                                keyField: String,
                                filterableFields: Option[Seq[String]],
                                sortableFields: Option[Seq[String]],
                                hiddenFields: Option[Seq[String]],
                                searchableFields: Option[Seq[String]],
                                facetableFields: Option[Seq[String]],
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

  final def isFacetable(field: String): Boolean = fieldNameExistsIn(field, facetableFields)

  /**
   * Evaluate if a fields is filterable
   * @param field field
   * @return true for filterable fields
   */

  final def isFilterable(field: String): Boolean = fieldNameExistsIn(field, filterableFields)

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

  final def isSearchable(field: String): Boolean = fieldNameExistsIn(field, searchableFields)

  /**
   * Evaluate if a fields is sortable
   * @param field field
   * @return true for sortable fields
   */

  final def isSortable(field: String): Boolean = fieldNameExistsIn(field, sortableFields)

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
   * Map of features and their predicates
   */

  private lazy val featuresAndPredicates: Map[SearchFieldFeature, String => Boolean] = Map(
    SearchFieldFeature.FACETABLE -> isFacetable,
    SearchFieldFeature.FILTERABLE -> isFilterable,
    SearchFieldFeature.HIDDEN -> isHidden,
    SearchFieldFeature.KEY -> isKey,
    SearchFieldFeature.SEARCHABLE -> isSearchable,
    SearchFieldFeature.SORTABLE -> isSortable
  )

  /**
   * Feature we want to explicitly disable if not marked by the user
   */

  private lazy val featuresToDisableIfNotPresent: Set[SearchFieldFeature] = Set(
    SearchFieldFeature.FACETABLE,
    SearchFieldFeature.FILTERABLE,
    SearchFieldFeature.SEARCHABLE,
    SearchFieldFeature.SORTABLE
  )

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

        // Collect the set of features to enable/disable
        val featuresToEnable = featuresAndPredicates.collect {
          case (k, v) if v.apply(structField.name) => k
        }.toSeq

        val featuresToDisable = featuresAndPredicates.collect {
          case (k, v) if !v.apply(structField.name) &&
            featuresToDisableIfNotPresent.contains(k) => k
        }.toSeq

        searchField.enableFeatures(featuresToEnable: _*)
          .disableFeatures(featuresToDisable: _*)
    }
  }
}
