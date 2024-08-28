package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException
import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.{SearchSparkConversionRules, SparkInternalConverter}
import com.github.jarol.azure.search.spark.sql.connector.schema.toSearchFieldOperations
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.{StructField, StructType}

class SearchScan private (private val schema: StructType,
                          private val readConfig: ReadConfig,
                          private val converters: Map[String, SparkInternalConverter])
  extends Scan {

  override def readSchema(): StructType = schema

  override def toBatch: Batch = new SearchBatch(readConfig, converters)
}

object SearchScan {

  def apply(schema: StructType,
            searchFields: Seq[SearchField],
            readConfig: ReadConfig): SearchScan = {

    val sparkAndSearchFields: Map[StructField, SearchField] = schema.map {
      spField => (
        spField,
        searchFields.collectFirst {
          case seField if seField.sameNameOf(spField) => seField
        })
    }.collect {
      case (spField, Some(seField)) => (spField, seField)
    }.toMap

    val convertersMap: Map[String, SparkInternalConverter] = sparkAndSearchFields.map {
      case (sparkField, searchField) => (
        sparkField.name,
        SearchSparkConversionRules
          .safeRuleFor(sparkField.dataType, searchField)
          .map(_.converter())
      )
    }.collect {
      case (name, Some(converter)) => (name, converter)
    }

    val schemaFieldsWithoutConversionRule: Set[String] = sparkAndSearchFields
      .keySet.map(_.name)
      .diff(convertersMap.keySet)

    if (schemaFieldsWithoutConversionRule.nonEmpty) {
      val size = schemaFieldsWithoutConversionRule.size
      val message = schemaFieldsWithoutConversionRule.mkString("(", ", ", ")")
      throw new AzureSparkException(s"Could not detect a conversion rule for $size fields $message")
    }

    new SearchScan(
      schema,
      readConfig,
      convertersMap
    )
  }
}
