package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException
import com.github.jarol.azure.search.spark.sql.connector.schema._
import org.apache.spark.sql.types.DataType

case object SearchSparkConversionRules {

  def safeConversionRuleFor(dataType: DataType, searchField: SearchField): Option[SearchSparkConversionRule] = {

    val searchFieldType = searchField.getType
    if (searchFieldType.isAtomic) {
      AtomicInferSchemaRules.safeRuleForTypes(
        dataType,
        searchFieldType
      ).orElse(
        AtomicSchemaConversionRules.safeRuleForTypes(
          dataType,
          searchFieldType
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

  final def unsafeRuleForTypes(dataType: DataType, searchField: SearchField): SearchSparkConversionRule = {

    safeConversionRuleFor(dataType, searchField) match {
      case Some(value) => value
      case None => throw new AzureSparkException(s"Could not find a conversion rule " +
        s"for field ${searchField.getName} (Search type: ${searchField.getType}, Spark type: ${dataType.typeName})")
    }
  }
}
