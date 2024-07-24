package com.github.jarol.azure.search.spark.sql.connector

import com.azure.core.credential.AzureKeyCredential
import com.azure.search.documents.indexes.SearchIndexClientBuilder

import scala.collection.JavaConverters._

object Main
  extends App {

  val searchClient = new SearchIndexClientBuilder()
    .endpoint("https://lohrwkpeacss01.search.windows.net")
    .credential(new AzureKeyCredential("3F491488E774609119C10968C6D47634"))
    .buildClient()

  val response = searchClient.getIndex("1721203770598-personnel-list")
  println(response.getFields.asScala.map(
    _.getName
  ).mkString(", ")
  )
}
