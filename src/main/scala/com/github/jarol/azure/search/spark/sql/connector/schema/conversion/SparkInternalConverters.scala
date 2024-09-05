package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.schema.{SchemaUtils, toSearchFieldOperations, toSearchTypeOperations}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

object SparkInternalConverters {

  def safeConverterFor(structField: StructField, searchField: SearchField): Option[SparkInternalConverter] = {

    val (sparkType, searchFieldType) = (structField.dataType, searchField.getType)
    if (!searchField.sameNameOf(structField)) {
      None
    } else {
      if (searchFieldType.isAtomic) {
        converterForAtomicTypes(sparkType, searchFieldType)
      } else if (searchFieldType.isCollection) {

        // Evaluate rule for the inner type
        val searchInnerType = searchFieldType.unsafelyExtractCollectionType
        sparkType match {
          case ArrayType(sparkInternalType, _) => safeConverterFor(
            StructField("array", sparkInternalType),
            new SearchField("array", searchInnerType)
          ).map(ArrayConverter)
          case _ => None
        }
      } else if (searchFieldType.isComplex) {

        // Build a rule that wraps subField conversion rules
        sparkType match {
          case StructType(sparkSubFields) =>

            val searchSubFields = JavaScalaConverters.listToSeq(searchField.getFields)
            val convertersMap: Map[String, SparkInternalConverter] = SchemaUtils
              .matchNamesakeFields(sparkSubFields, searchSubFields)
              .map {
                case (k, v) => (k.name, safeConverterFor(k, v))
              }.collect {
                case (k, Some(v)) => (k, v)
              }

            if (SchemaUtils.allSchemaFieldsExist(sparkSubFields, searchSubFields)) {
              Some(ComplexConverter(convertersMap))
            } else {
              None
            }
          case _ => None
        }
      } else if (searchFieldType.isGeoPoint) {
        Some(GeoPointRule.converter())
      } else {
        None
      }
    }
  }

  private def converterForAtomicTypes(spark: DataType, search: SearchFieldDataType): Option[SparkInternalConverter] = {

    // For atomic types, there should exist either an inference rule or a conversion rule
    AtomicInferSchemaRules.safeRuleForTypes(
      spark, search
    ).orElse(
      AtomicSchemaConversionRules.safeRuleForTypes(
        spark, search
      )
    ).map(_.converter())
  }
}
