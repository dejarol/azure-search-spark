package com.github.jarol.azure.search.spark.sql.connector.write

import com.github.jarol.azure.search.spark.sql.connector.core.schema.SearchFieldFeature

/**
 * Assertion for testing the enabling process of a [[SearchFieldFeature]]
 */

trait FeatureEnabledAssertion {

  /**
   * Get the feature to test
   * @return feature to test
   */

  def feature: SearchFieldFeature

  /**
   * Evaluate if a field should be enabled for this instance's feature given some field options
   * @param options field options
   * @param field field to test
   * @return true if this instance's feature should be enabled, according to the options, for given field
   */

  def shouldBeEnabled(options: SearchFieldsOptions, field: StructField): Boolean

}

object FeatureEnabledAssertion {

  case object Facetable extends FeatureEnabledAssertion {
    override def feature: SearchFieldFeature = SearchFieldFeature.FACETABLE
    override def shouldBeEnabled(options: SearchFieldsOptions, field: StructField): Boolean = options.isFacetable(field)
  }

  case object Filterable extends FeatureEnabledAssertion {
    override def feature: SearchFieldFeature = SearchFieldFeature.FILTERABLE
    override def shouldBeEnabled(options: SearchFieldsOptions, field: StructField): Boolean = options.isFilterable(field)
  }

  case object Hidden extends FeatureEnabledAssertion {
    override def feature: SearchFieldFeature = SearchFieldFeature.HIDDEN
    override def shouldBeEnabled(options: SearchFieldsOptions, field: StructField): Boolean = options.isHidden(field)
  }

  case object Key extends FeatureEnabledAssertion {
    override def feature: SearchFieldFeature = SearchFieldFeature.KEY
    override def shouldBeEnabled(options: SearchFieldsOptions, field: StructField): Boolean = options.isKey(field)
  }

  case object Searchable extends FeatureEnabledAssertion {
    override def feature: SearchFieldFeature = SearchFieldFeature.SEARCHABLE
    override def shouldBeEnabled(options: SearchFieldsOptions, field: StructField): Boolean = options.isSearchable(field)
  }

  case object Sortable extends FeatureEnabledAssertion {
    override def feature: SearchFieldFeature = SearchFieldFeature.SORTABLE
    override def shouldBeEnabled(options: SearchFieldsOptions, field: StructField): Boolean = options.isSortable(field)
  }
}
