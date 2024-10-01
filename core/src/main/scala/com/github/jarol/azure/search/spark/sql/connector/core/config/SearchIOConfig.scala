package com.github.jarol.azure.search.spark.sql.connector.core.config

import com.azure.core.credential.AzureKeyCredential
import com.azure.search.documents.SearchClient
import com.azure.search.documents.indexes.models.{SearchField, SearchIndex}
import com.azure.search.documents.indexes.{SearchIndexClient, SearchIndexClientBuilder}
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.utils.SearchUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * Parent class for read/write configurations
 * @param localOptions  options passed to the dataSource
 * @param globalOptions options retrieved from the underlying Spark configuration
 */

class SearchIOConfig(override protected val localOptions: CaseInsensitiveMap[String],
                     override protected val globalOptions: CaseInsensitiveMap[String])
  extends SearchConfig(localOptions, globalOptions)
    with IOConfig {

  /**
   * Create a new instance from two simple maps
   * @param locals options passed to the dataSource
   * @param globals options from the underlying Spark configuration
   */

  def this(locals: Map[String, String], globals: Map[String, String]) = {

    this(
      CaseInsensitiveMap(locals),
      CaseInsensitiveMap(globals)
    )
  }

  override def getEndpoint: String = unsafelyGet(IOConfig.END_POINT_CONFIG)

  override def getAPIkey: String = unsafelyGet(IOConfig.API_KEY_CONFIG)

  override def getIndex: String = unsafelyGet(IOConfig.INDEX_CONFIG)

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

object SearchIOConfig {

  /**
   * Extract all options from given SparkConf that starts with the prefix of a usage mode
   * @param sparkConf an instance of [[SparkConf]]
   * @param mode usage mode
   * @return all key-value pairs whose keys start with given mode prefix
   */

  private[config] def allConfigsForMode(sparkConf: SparkConf, mode: UsageMode): Map[String, String] = {

   sparkConf
      .getAllWithPrefix(mode.prefix())
      .toMap
  }

  /**
   * Retrieve all options related to a mode from the active SparkSession (if any)
   * @param mode usage mode
   * @return an empty Map if no [[SparkSession]] is active, all options related to the mode otherwise
   */

  def allConfigsFromActiveSessionForMode(mode: UsageMode): Map[String, String] = {

    SparkSession.getActiveSession match {
      case Some(value) => allConfigsForMode(value.sparkContext.getConf, mode)
      case None => Map.empty
    }
  }
}
