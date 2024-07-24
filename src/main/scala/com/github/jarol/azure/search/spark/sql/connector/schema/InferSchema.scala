package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.core.credential.AzureKeyCredential
import com.azure.search.documents.indexes.{SearchIndexClient, SearchIndexClientBuilder}
import com.github.jarol.azure.search.spark.sql.connector.JavaToScala
import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import org.apache.spark.sql.types.StructType

object InferSchema {

  def inferSchema(options: Map[String, String]): StructType = {

   val readConfig = ReadConfig(options)
   val client: SearchIndexClient = new SearchIndexClientBuilder()
     .endpoint(readConfig.getEndpoint)
     .credential(new AzureKeyCredential(readConfig.getAPIkey))
     .buildClient

    SchemaUtils.asSchema(
      JavaToScala.listToSeq(
        client.getIndex(readConfig.getIndex)
          .getFields)
    )
  }
}
