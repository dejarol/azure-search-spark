package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchIndex
import com.github.jarol.azure.search.spark.sql.connector.schema._
import org.apache.spark.sql.types.StructField

/**
 * Options for creating a Search index
 * @param name index name
 * @param keyField field to be used as document key
 * @param filterableFields list of fields that should be filterable
 * @param sortableFields list of fields that should be sortable
 * @param hiddenFields list of fields that should be hidden (i.e. non-retrievable)
 * @param searchableFields list of fields that should be searchable
 * @param facetableFields list of fields that should be facetable
 * @param indexActionColumn column name for retrieving a per-document [[com.azure.search.documents.models.IndexActionType]]
 */

case class CreateSearchIndexOptions(name: String,
                                    keyField: String,
                                    filterableFields: Option[Seq[String]],
                                    sortableFields: Option[Seq[String]],
                                    hiddenFields: Option[Seq[String]],
                                    searchableFields: Option[Seq[String]],
                                    facetableFields: Option[Seq[String]],
                                    indexActionColumn: Option[String]) {

  /**
   * Create a Search index definition, starting from a Spark schema
   * @param schema Dataframe schema
   * @return a Search index definition
   */

  def toSearchIndex(schema: Seq[StructField]): SearchIndex = {

    val fieldNameInList: (Option[Seq[String]], StructField, SearchFieldFeature) => Option[SearchFieldFeature] =
      (list, field, feature) => list match {
        case Some(value) => if (value.exists(_.equalsIgnoreCase(field.name))) {
          Some(feature)
        } else None
        case None => None
      }

    val filteredSchema: Seq[StructField] = indexActionColumn match {
      case Some(value) => schema.filterNot {
        _.name.equalsIgnoreCase(value)
      }
      case None => schema
    }

    val indexFields = filteredSchema.map {
      s =>

        SchemaUtils.toSearchField(s)
          .maybeSetKey(keyField)
          .maybeSetFilterable(filterableFields)
          .maybeSetSortable(sortableFields)
          .maybeSetHidden(hiddenFields)
          .maybeSetSearchable(searchableFields)
          .maybeSetFacetable(facetableFields)
    }

    new SearchIndex(name)
      .setFields(indexFields: _*)
  }
}
