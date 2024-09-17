package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.SearchField
import org.apache.spark.sql.types.StructField

/**
 * Set of utilities for dealing with [[SearchField]](s)
 * @param field search field
 */

class SearchFieldOperations(private val field: SearchField) {

  /**
   * Evaluates if this field has the same name with respect to given Spark field
   * @param sparkField spark field
   * @return true for same names (case-insensitive)
   */

  final def sameNameOf(sparkField: StructField): Boolean = field.getName.equalsIgnoreCase(sparkField.name)

  final def enableFeatures(features: SearchFieldFeature*): SearchField = {

    features.foldLeft(field) {
      case (field, feature) =>
        feature.enablingFunction.apply(field, true)
    }
  }

  /**
   * Maybe enable a field property, if its name is among the given list
   * @param names field names
   * @param update function to use for enabling a field property
   * @return this field, maybe with one of its property enabled (i.e. set to true)
   */

  private def maybeEnableFieldProperty(names: Option[Seq[String]],
                                       update: SearchField => SearchField): SearchField = {

    val fieldNameInList = names.exists {
      _.exists(_.equalsIgnoreCase(field.getName))
    }

    if (fieldNameInList) {
      update(field)
    } else {
      field
    }
  }

  /**
   * Set the key property to true for this field if its name matches
   * @param name name to match
   * @return this Search field maybe with key property set to true
   */

  final def maybeSetKey(name: String): SearchField = {

    if (field.getName.equalsIgnoreCase(name)) {
      field.setKey(true)
    } else {
      field
    }
  }

  /**
   * Maybe set this field as filterable
   * @param fields field names
   * @return this field, maybe set as filterable
   */

  final def maybeSetFilterable(fields: Option[Seq[String]]): SearchField = {

    maybeEnableFieldProperty(
      fields,
      _.setFilterable(true)
    )
  }

  /**
   * Maybe set this field as sortable
   * @param fields field names
   * @return this field, maybe set as sortable
   */

  final def maybeSetSortable(fields: Option[Seq[String]]): SearchField = {

    maybeEnableFieldProperty(
      fields,
      _.setSortable(true)
    )
  }

  /**
   * Maybe set this field as hidden
   * @param fields field names
   * @return this field, maybe set as filterable
   */

  final def maybeSetHidden(fields: Option[Seq[String]]): SearchField = {

    maybeEnableFieldProperty(
      fields,
      _.setHidden(true)
    )
  }

  /**
   * Maybe set this field as searchable
   * @param fields field names
   * @return this field, maybe set as searchable
   */

  final def maybeSetSearchable(fields: Option[Seq[String]]): SearchField = {

    maybeEnableFieldProperty(
      fields,
      _.setSearchable(true)
    )
  }

  /**
   * Maybe set this field as facetable
   * @param fields field names
   * @return this field, maybe set as facetable
   */

  final def maybeSetFacetable(fields: Option[Seq[String]]): SearchField = {

    maybeEnableFieldProperty(
      fields,
      _.setFacetable(true)
    )
  }
}
