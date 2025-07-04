package io.github.dejarol.azure.search.spark.connector.core.config

import com.azure.core.credential.AzureKeyCredential
import com.azure.search.documents.SearchClient
import com.azure.search.documents.indexes.models.{SearchField, SearchIndex}
import com.azure.search.documents.indexes.{SearchIndexClient, SearchIndexClientBuilder}
import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.core.utils.SearchClients
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
   * Get a [[SearchClient]] for
   *  - searching your indexed documents
   *  - adding, updating or deleting documents from an index
   * @return a Search index
   */

  private def getSearchClient: SearchClient = getSearchIndexClient.getSearchClient(getIndex)

  /**
   * Perform an action using this instance's [[com.azure.search.documents.indexes.SearchIndexClient]], and get the result
   * @param function action to perform
   * @tparam T action return type
   * @return the action result
   */

  final def withSearchIndexClientDo[T](function: SearchIndexClient => T): T = function.apply(getSearchIndexClient)

  /**
   * Perform an action using this instance's [[com.azure.search.documents.SearchClient]], and get the result
   * @param function action to perform
   * @tparam T action return type
   * @return the action result
   */

  final def withSearchClientDo[T](function: SearchClient => T): T = function.apply(getSearchClient)

  /**
   * Evaluate if an index exists
   * @param name index name
   * @return true if the index exists
   * @since 0.11.0
   */

  final def indexExists(name: String): Boolean = {

    withSearchIndexClientDo {
      client =>
        SearchClients.indexExists(client, name)
    }
  }

  /**
   * Evaluate if this instance's target index exists.
   * The index name will be retrieved either from
   *  - option <code>path</code> or
   *  - option <code>index</code>
   * @return true if the index exist
   */

  final def indexExists: Boolean = indexExists(getIndex)

  /**
   * Get the list of existing Search indexes
   * @return collection of existing indexes
   * @since 0.11.0
   */

  final def listIndexes: Seq[SearchIndex] = {

    withSearchIndexClientDo {
      client =>
        JavaScalaConverters.listToSeq(
          SearchClients.listIndexes(client)
        )
    }
  }

  /**
   * Get the fields defined in a Search index
   * @param name index name
   * @return a collection of index fields
   * @since 0.11.0
   */

  final def getSearchIndexFields(name: String): Seq[SearchField] = {

    withSearchIndexClientDo {
      client => JavaScalaConverters.listToSeq(
        client.getIndex(name).getFields
      )
    }
  }

  /**
   * Get the fields defined by this instance's target index.
   * The index name will be retrieved either from
   *  - option <code>path</code> or
   *  - option <code>index</code>
   * @return a collection of index fields
   */

  final def getSearchIndexFields: Seq[SearchField] = getSearchIndexFields(getIndex)
}