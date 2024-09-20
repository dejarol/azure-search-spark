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

  final def isKey(field: StructField): Boolean = field.name.equalsIgnoreCase(keyField)

  /**
   * Evaluate if a field name exists within an optional list of field names
   * @param field field
   * @param fieldNames optional list of field names
   * @return true for existing fields
   */

  private def fieldNameExistsIn(field: StructField, fieldNames: Option[Seq[String]]): Boolean = {

    fieldNames.exists {
      _.exists {
        _.equalsIgnoreCase(field.name)
      }
    }
  }

  /**
   * Evaluate if a field is facetable
   * @param field field
   * @return true for facetable fields
   */

  final def isFacetable(field: StructField): Boolean = fieldNameExistsIn(field, facetableFields)

  /**
   * Evaluate if a fields is filterable
   * @param field field
   * @return true for filterable fields
   */

  final def isFilterable(field: StructField): Boolean = fieldNameExistsIn(field, filterableFields)

  /**
   * Evaluate if a fields is hidden
   * @param field field
   * @return true for hidden fields
   */

  final def isHidden(field: StructField): Boolean = fieldNameExistsIn(field, hiddenFields)

  /**
   * Evaluate if a fields is searchable
   * @param field field
   * @return true for searchable fields
   */

  final def isSearchable(field: StructField): Boolean = fieldNameExistsIn(field, searchableFields)

  /**
   * Evaluate if a fields is sortable
   * @param field field
   * @return true for sortable fields
   */

  final def isSortable(field: StructField): Boolean = fieldNameExistsIn(field, sortableFields)

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

  private lazy val featuresAndPredicates: Map[SearchFieldFeature, StructField => Boolean] = Map(
    SearchFieldFeature.FACETABLE -> isFacetable,
    SearchFieldFeature.FILTERABLE -> isFilterable,
    SearchFieldFeature.HIDDEN -> isHidden,
    SearchFieldFeature.KEY -> isKey,
    SearchFieldFeature.SEARCHABLE -> isSearchable,
    SearchFieldFeature.SORTABLE -> isSortable
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
        // Collect the set of features to enable
        val featuresToEnable: Seq[SearchFieldFeature] = featuresAndPredicates
          .collect {
            case (k, v) if v.apply(structField) => k
          }.toSeq

        SchemaUtils.toSearchField(structField)
          .enableFeatures(featuresToEnable: _*)
    }
  }
}
