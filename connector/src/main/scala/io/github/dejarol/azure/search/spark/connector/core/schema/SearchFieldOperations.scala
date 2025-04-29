package io.github.dejarol.azure.search.spark.connector.core.schema

import com.azure.search.documents.indexes.models.SearchField
import io.github.dejarol.azure.search.spark.connector.core.{EntityDescription, JavaScalaConverters}

import java.util.Objects

class SearchFieldOperations(private val searchField: SearchField)
  extends SubFieldsSupplier[SearchField]
    with EntityDescription {

  override def description(): String = s"Search field ${searchField.getName} (type ${searchField.getType.toString})"

  override def safeSubFields: Option[Seq[SearchField]] = {

    val subFields = searchField.getFields
    if (Objects.isNull(subFields) || subFields.isEmpty) {
      None
    } else {
      Some(
        JavaScalaConverters.listToSeq(
          subFields
        )
      )
    }
  }

  /**
   * Evaluate if a feature is enabled on this field
   * @param feature feature
   * @return true for enabled features
   */

  final def isEnabledFor(feature: SearchFieldFeature): Boolean = feature.isEnabledOnField(searchField)
}