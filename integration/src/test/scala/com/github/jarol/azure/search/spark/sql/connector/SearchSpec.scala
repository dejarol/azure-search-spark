package com.github.jarol.azure.search.spark.sql.connector

import com.azure.core.credential.AzureKeyCredential
import com.azure.search.documents.SearchClient
import com.azure.search.documents.indexes.models.{SearchField, SearchIndex}
import com.azure.search.documents.indexes.{SearchIndexClient, SearchIndexClientBuilder}
import com.azure.search.documents.models.SearchOptions
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, JavaScalaConverters}
import com.github.jarol.azure.search.spark.sql.connector.core.utils.SearchUtils

trait SearchSpec
  extends BasicSpec {

  protected final lazy val SEARCH_END_POINT = sys.env("AZURE_SEARCH_ENDPOINT")
  protected final lazy val SEARCH_API_KEY = sys.env("AZURE_SEARCH_API_KEY")
  protected final lazy val KEY_CREDENTIALS = new AzureKeyCredential(SEARCH_API_KEY)

  protected final lazy val searchIndexClient: SearchIndexClient = new SearchIndexClientBuilder()
      .endpoint(SEARCH_END_POINT)
      .credential(KEY_CREDENTIALS)
      .buildClient

  protected final def getSearchIndex(name: String): SearchIndex = searchIndexClient.getIndex(name)

  protected final def getSearchClient(name: String): SearchClient = searchIndexClient.getSearchClient(name)

  protected final def indexExists(name: String): Boolean = {

   SearchUtils.indexExists(
     searchIndexClient,
     name
   )
  }

  protected final def countDocumentsForIndex(name: String): Long = {

    SearchUtils.getSearchPagedIterable(
      getSearchClient(name),
      new SearchOptions().setIncludeTotalCount(true)
    ).getTotalCount
  }

  protected final def dropIndex(name: String): Unit = {

    searchIndexClient.deleteIndex(name)
  }

  protected final def dropIndexIfExists(name: String): Unit = {

    if (indexExists(name)) {
      dropIndex(name)
    }
  }

  protected final def getIndexFields(name: String): Seq[SearchField] = {

    JavaScalaConverters.listToSeq(
      getSearchIndex(name).getFields
    )
  }
}

