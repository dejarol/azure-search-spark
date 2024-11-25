package com.github.jarol.azure.search.spark.sql.connector.core.schema

import com.azure.search.documents.indexes.models.SearchField

/**
 * Collection of methods for creating [[SearchFieldAction]](s)
 */

object SearchFieldActions {

  /**
   * Action for enabling a feature
   * @param feature feature to enable
   */

  private case class EnableFeature(private val feature: SearchFieldFeature)
    extends SearchFieldAction {
    override final def apply(field: SearchField): SearchField = {
      feature.enableOnField(field)
    }
  }

  /**
   * Action for disabling a feature
   * @param feature feature to disable
   */

  private case class DisableFeature(private val feature: SearchFieldFeature)
    extends SearchFieldAction {
    override def apply(field: SearchField): SearchField = {
      feature.disableOnField(field)
    }
  }

  /**
   * Get an instance of [[SearchFieldAction]] for enabling a feature
   * @param feature feature to enable
   * @return an action for enabling a feature
   */

  final def forEnablingFeature(feature: SearchFieldFeature): SearchFieldAction = EnableFeature(feature)

  /**
   * Get an instance of [[SearchFieldAction]] for disabling a feature
   * @param feature feature to disable
   * @return an action for disabling a feature
   */

  final def forDisablingFeature(feature: SearchFieldFeature): SearchFieldAction = DisableFeature(feature)
}
