package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.SearchIndexClient
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.clients.ClientFactory
import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import org.apache.spark.sql.types.StructType

/**
 * Object for inferring the schema of an Azure Cognitive Search index
 */

object InferSchema {

  /**
   * Infer the schema by reading options provided to a [[org.apache.spark.sql.DataFrameReader]]
   * @param options options passed to the reader via [[org.apache.spark.sql.DataFrameReader.option()]] method
   * @return the schema of target Search index
   */

  def inferSchema(options: Map[String, String]): StructType = {

   val readConfig = ReadConfig(options)
   val client: SearchIndexClient = ClientFactory.indexClient(readConfig)
    SchemaUtils.asStructType(
      JavaScalaConverters.listToSeq(
        client.getIndex(readConfig.getIndex)
          .getFields)
    )
  }
}
