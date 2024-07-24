package com.github.jarol.azure.search.spark.sql.connector

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class SearchTableProvider
  extends TableProvider
    with DataSourceRegister {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = ???

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = new SearchTable(schema)

  override def shortName(): String = SearchTableProvider.SHORT_NAME
}

object SearchTableProvider {

  final val SHORT_NAME = "azuresearch"
}