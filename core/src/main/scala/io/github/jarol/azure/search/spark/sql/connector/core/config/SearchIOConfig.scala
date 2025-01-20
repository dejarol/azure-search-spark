package io.github.jarol.azure.search.spark.sql.connector.core.config

import com.azure.core.credential.AzureKeyCredential
import com.azure.search.documents.SearchClient
import com.azure.search.documents.indexes.models.{SearchField, SearchIndex}
import com.azure.search.documents.indexes.{SearchIndexClient, SearchIndexClientBuilder}
import io.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import io.github.jarol.azure.search.spark.sql.connector.core.utils.SearchClients
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * Parent class for read/write configurations
 * @param options  options passed to the dataSource
 */

class SearchIOConfig(override protected val options: CaseInsensitiveMap[String])
  extends SearchConfig(options)
    with IOConfig {

  /**
   * Create a new instance from a simple map
   * @param options options passed to the dataSource
   */

  def this(options: Map[String, String]) = this(CaseInsensitiveMap(options))

  /**
   * Get the target Search endpoint, at either local of config level
   * @return the target Search endpoint
   */

  override def getEndpoint: String = unsafelyGetLocalOrSessionConfiguration(IOConfig.END_POINT_CONFIG)

  /**
   * Get the target Search API key, at either local of config level
   * @return the target Search API key
   */

  override def getAPIkey: String = unsafelyGetLocalOrSessionConfiguration(IOConfig.API_KEY_CONFIG)

  /**
   * Retrieve the target index name, by looking for the value of options
   *  - 'path' (defined if the user specified the option or passed an argument to DataframeReader/Writer load/save method
   *  - 'index' (defined if the user specified the option)
   * @return the target Search index name
   */

  override def getIndex: String = {

    get("path")
      .orElse(get(IOConfig.INDEX_CONFIG))
      .getOrElse {
        throw SearchConfig.exceptionForMissingOption(
          "path",
          None,
          Some(ConfigException.PATH_OR_INDEX_SUPPLIER)
        )
      }
  }

  /**
   * Special method for retrieving a mandatory option that could be set
   * either locally (by passing it to DataframeReader/Writer) or at the session level
   * @param key mandatory option
   * @throws ConfigException if the option misses
   * @return the option value
   */

  @throws[ConfigException]
  private def unsafelyGetLocalOrSessionConfiguration(key: String): String = {

    unsafelyGet(
      key,
      None,
      Some(ConfigException.LOCAL_OR_SESSION_CONFIGURATION_MESSAGE_SUPPLIER)
    )
  }

  /**
   * Create a [[SearchIndexClient]] for creating, deleting, updating, or configure a search index
   * @return a Search index client
   */

  private def getSearchIndexClient: SearchIndexClient = {

    new SearchIndexClientBuilder()
      .endpoint(getEndpoint)
      .credential(new AzureKeyCredential(getAPIkey))
      .buildClient
  }

  /**
   * Get a [[SearchIndex]] definition
   * @return the definition of a Search index
   */

  private def getSearchIndex: SearchIndex = getSearchIndexClient.getIndex(getIndex)

  /**
   * Get a [[SearchClient]] for
   *  - searching your indexed documents
   *  - adding, updating or deleting documents from an index
   * @return a Search index
   */

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
          SearchClients.indexExists(client, getIndex)
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