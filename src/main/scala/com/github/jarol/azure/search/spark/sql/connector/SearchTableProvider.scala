package com.github.jarol.azure.search.spark.sql.connector

import com.github.jarol.azure.search.spark.sql.connector.schema.InferSchema
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

/**
 * Table provider that provides a [[Table]] able to deal with Azure Cognitive Search indexes
 */

class SearchTableProvider
  extends TableProvider
    with DataSourceRegister {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {

    InferSchema.inferSchema(
      JavaScalaConverters.javaMapToScalaMap(options)
    )
  }

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = new SearchTable(schema)

  override def shortName(): String = SearchTableProvider.SHORT_NAME
}

object SearchTableProvider {

  /**
   * Datasource name
   */

  final val SHORT_NAME: String = "azuresearch"
}