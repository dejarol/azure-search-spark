package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.core.schema.SearchFieldFeature
import org.apache.spark.sql.types.StructField

import java.lang.{Boolean => JBoolean}

/**
 * Assertion for testing the enabling/disabling process of a [[SearchFieldFeature]]
 */

trait FeatureAssertion {

  /**
   * Get the feature to test
   * @return feature to test
   */

  def feature: SearchFieldFeature

  /**
   * Returns true for instances whose features should be disabled on a field
   * @return true for all features but [[SearchFieldFeature.HIDDEN]] and [[SearchFieldFeature.KEY]]
   */

  final def refersToDisablingFeature: Boolean = {

    feature match {
      case SearchFieldFeature.KEY | SearchFieldFeature.HIDDEN => false
      case _ => true
    }
  }

  /**
   * Evaluate if a field matches with this assertion type
   * @param field field to test
   * @param options search field options
   * @return true if the field should be altered
   */

  def shouldBeAltered(field: StructField, options: SearchFieldsCreationOptions): Boolean

  def getFeatureValue(searchField: SearchField): Option[JBoolean]
}

object FeatureAssertion {

  case object FACETABLE extends FeatureAssertion {
    override def feature: SearchFieldFeature = SearchFieldFeature.FACETABLE
    override def shouldBeAltered(field: StructField, options: SearchFieldsCreationOptions): Boolean = options.nonFacetable(field.name)
    override def getFeatureValue(searchField: SearchField): Option[JBoolean] = Option(searchField.isFacetable)
  }

  case object FILTERABLE extends FeatureAssertion {
    override def feature: SearchFieldFeature = SearchFieldFeature.FILTERABLE
    override def shouldBeAltered(field: StructField, options: SearchFieldsCreationOptions): Boolean = options.nonFilterable(field.name)
    override def getFeatureValue(searchField: SearchField): Option[JBoolean] = Option(searchField.isFilterable)
  }

  case object HIDDEN extends FeatureAssertion {
    override def feature: SearchFieldFeature = SearchFieldFeature.HIDDEN
    override def shouldBeAltered(field: StructField, options: SearchFieldsCreationOptions): Boolean = options.isHidden(field.name)
    override def getFeatureValue(searchField: SearchField): Option[JBoolean] = Option(searchField.isHidden)
  }

  case object KEY extends FeatureAssertion {
    override def feature: SearchFieldFeature = SearchFieldFeature.KEY
    override def shouldBeAltered(field: StructField, options: SearchFieldsCreationOptions): Boolean = options.isKey(field.name)
    override def getFeatureValue(searchField: SearchField): Option[JBoolean] = Option(searchField.isKey)
  }

  case object SEARCHABLE extends FeatureAssertion {
    override def feature: SearchFieldFeature = SearchFieldFeature.SEARCHABLE
    override def shouldBeAltered(field: StructField, options: SearchFieldsCreationOptions): Boolean = options.nonSearchable(field.name)
    override def getFeatureValue(searchField: SearchField): Option[JBoolean] = Option(searchField.isSearchable)
  }

  case object SORTABLE extends FeatureAssertion {
    override def feature: SearchFieldFeature = SearchFieldFeature.SORTABLE
    override def shouldBeAltered(field: StructField, options: SearchFieldsCreationOptions): Boolean = options.nonSortable(field.name)
    override def getFeatureValue(searchField: SearchField): Option[JBoolean] = Option(searchField.isSortable)
  }
}
