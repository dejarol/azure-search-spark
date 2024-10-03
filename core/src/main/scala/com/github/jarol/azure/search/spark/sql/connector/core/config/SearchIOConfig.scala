package com.github.jarol.azure.search.spark.sql.connector.core.config

import com.azure.core.credential.AzureKeyCredential
import com.azure.search.documents.SearchClient
import com.azure.search.documents.indexes.models.{SearchField, SearchIndex}
import com.azure.search.documents.indexes.{SearchIndexClient, SearchIndexClientBuilder}
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.utils.SearchUtils
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * Parent class for read/write configurations
 * @param dsOptions  options passed to the dataSource
 */

class SearchIOConfig(override protected val dsOptions: CaseInsensitiveMap[String])
  extends SearchConfig(dsOptions)
    with IOConfig {

  /**
   * Create a new instance from a simple map
   * @param dsOptions options passed to the dataSource
   */

  def this(dsOptions: Map[String, String]) = this(CaseInsensitiveMap(dsOptions))

  private def unsafelyGetDatasourceConfiguration(key: String): String = {

    unsafelyGet(
      key,
      None,
      Some(ConfigException.LOCAL_OR_SESSION_CONFIGURATION_MESSAGE_SUPPLIER)
    )
  }

  override def getEndpoint: String = unsafelyGetDatasourceConfiguration(IOConfig.END_POINT_CONFIG)

  override def getAPIkey: String = unsafelyGetDatasourceConfiguration(IOConfig.API_KEY_CONFIG)

  override def getIndex: String = {

    get("path")
      .orElse(get(IOConfig.INDEX_CONFIG))
      .getOrElse {
        throw SearchConfig.exceptionForMissingKey(
          "path",
          None,
          Some(ConfigException.PATH_OR_INDEX_SUPPLIER)
        )
      }
  }

  private def getSearchIndexClient: SearchIndexClient = {

    new SearchIndexClientBuilder()
      .endpoint(getEndpoint)
      .credential(new AzureKeyCredential(getAPIkey))
      .buildClient
  }

  private def getSearchIndex: SearchIndex = getSearchIndexClient.getIndex(getIndex)

  private def getSearchClient: SearchClient = getSearchIndexClient.getSearchClient(getIndex)

  /**
   * Perform an action using this instance's [[SearchIndexClient]], and get the result
   * @param function action to perform
   * @tparam T action return type
   * @return the action result
   */

  final def withSearchIndexClientDo[T](function: SearchIndexClient => T): T = function.apply(getSearchIndexClient)

  /**
   * Perform an action using this instance's [[SearchIndex]], and get the result
   * @param function action to perform
   * @tparam T action return type
   * @return the action result
   */

  private final def withSearchIndexDo[T](function: SearchIndex => T): T = function.apply(getSearchIndex)

  /**
   * Perform an action using this instance's [[SearchClient]], and get the result
   * @param function action to perform
   * @tparam T action return type
   * @return the action result
   */

  final def withSearchClientDo[T](function: SearchClient => T): T = function.apply(getSearchClient)

  /**
   * Evaluate if this instance's index exists
   * @return true if the index exist
   */

  final def indexExists: Boolean = {

    withSearchIndexClientDo {
        client =>
          SearchUtils.indexExists(client, getIndex)
    }
  }

  /**
   * Get the list of Search index fields
   * @return list of Search index fields
   */

  final def getSearchIndexFields: Seq[SearchField] = {

    withSearchIndexDo {
      si =>
      JavaScalaConverters.listToSeq(
        si.getFields
      )
    }
  }
}