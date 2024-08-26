package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.schema._
import org.apache.spark.sql.types.DataType

case object SearchSparkConversionRules {

  def safeConversionRuleFor(searchField: SearchField, dataType: DataType): Option[SearchSparkConversionRule] = {

    val searchFieldType = searchField.getType
    if (searchFieldType.isAtomic) {
      AtomicInferSchemaRules.safeRuleFor(
        searchFieldType,
        dataType
      ).orElse(
        AtomicSchemaConversionRules.safeRuleFor(
          searchFieldType,
          dataType
        )
      )
    } else if (searchFieldType.isCollection) {
      Some(
        ArrayConversionRule(
          dataType,
          searchFieldType.unsafelyExtractCollectionType
        )
      )
    } else if (searchFieldType.isComplex) {
      Some(
        ComplexConversionRule(
          java.util.Collections.emptyList()
        )
      )
    } else if (searchFieldType.isGeoPoint) {
      Some(GeoPointRule)
    } else {
      None
    }
  }
}
