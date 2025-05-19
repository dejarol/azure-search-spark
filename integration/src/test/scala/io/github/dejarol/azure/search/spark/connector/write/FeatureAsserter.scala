package io.github.dejarol.azure.search.spark.connector.write

import com.azure.search.documents.indexes.models.SearchField
import io.github.dejarol.azure.search.spark.connector.core.schema.SearchFieldFeature

import java.lang.{Boolean => JBoolean}

/**
 * Assertion for testing the enabling/disabling process of a [[SearchFieldFeature]]
 */

trait FeatureAsserter {

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
   * Retrieve the feature flag from a field
   * @param searchField Search field
   * @return value of feature flag
   */

  def getFeatureValue(searchField: SearchField): Option[JBoolean]

  /**
   * Get the write config option suffix related to this feature
   * @return feature suffix
   */

  def suffix: String
}

object FeatureAsserter {

  case object FACETABLE extends FeatureAsserter {
    override def feature: SearchFieldFeature = SearchFieldFeature.FACETABLE
    override def getFeatureValue(searchField: SearchField): Option[JBoolean] = Option(searchField.isFacetable)
    override def suffix: String = SearchFieldCreationOptions.NON_FACETABLE_CONFIG
  }

  case object FILTERABLE extends FeatureAsserter {
    override def feature: SearchFieldFeature = SearchFieldFeature.FILTERABLE
    override def getFeatureValue(searchField: SearchField): Option[JBoolean] = Option(searchField.isFilterable)
    override def suffix: String = SearchFieldCreationOptions.NON_FILTERABLE_CONFIG
  }

  case object HIDDEN extends FeatureAsserter {
    override def feature: SearchFieldFeature = SearchFieldFeature.HIDDEN
    override def getFeatureValue(searchField: SearchField): Option[JBoolean] = Option(searchField.isHidden)
    override def suffix: String = SearchFieldCreationOptions.HIDDEN_FIELDS_CONFIG
  }

  case object KEY extends FeatureAsserter {
    override def feature: SearchFieldFeature = SearchFieldFeature.KEY
    override def getFeatureValue(searchField: SearchField): Option[JBoolean] = Option(searchField.isKey)
    override def suffix: String = SearchFieldCreationOptions.KEY_FIELD_CONFIG
  }

  case object SEARCHABLE extends FeatureAsserter {
    override def feature: SearchFieldFeature = SearchFieldFeature.SEARCHABLE
    override def getFeatureValue(searchField: SearchField): Option[JBoolean] = Option(searchField.isSearchable)
    override def suffix: String = SearchFieldCreationOptions.NON_SEARCHABLE_CONFIG
  }

  case object SORTABLE extends FeatureAsserter {
    override def feature: SearchFieldFeature = SearchFieldFeature.SORTABLE
    override def getFeatureValue(searchField: SearchField): Option[JBoolean] = Option(searchField.isSortable)
    override def suffix: String = SearchFieldCreationOptions.NON_SORTABLE_CONFIG
  }
}
