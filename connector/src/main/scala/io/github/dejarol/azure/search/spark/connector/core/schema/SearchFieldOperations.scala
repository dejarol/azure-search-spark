package io.github.dejarol.azure.search.spark.connector.core.schema

import com.azure.search.documents.indexes.models.SearchField
import io.github.dejarol.azure.search.spark.connector.core.{EntityDescription, JavaScalaConverters}

/**
 * Supplier implementation for extracting subFields from a [[com.azure.search.documents.indexes.models.SearchField]]
 * @param searchField a Search field
 */

class SearchFieldOperations(private val searchField: SearchField)
  extends SubFieldsSupplier[SearchField]
    with EntityDescription {

  override def description(): String = s"Search field ${searchField.getName} (type ${searchField.getType.toString})"

  override def safeSubFields: Option[Seq[SearchField]] = {

    Option(searchField.getFields)
      .filterNot {
        _.isEmpty
      }.map {
        JavaScalaConverters.listToSeq
      }
  }

  /**
   * Evaluate if a feature is enabled on this field
   * @param feature feature
   * @return true for enabled features
   */

  final def isEnabledFor(feature: SearchFieldFeature): Boolean = feature.isEnabledOnField(searchField)
}