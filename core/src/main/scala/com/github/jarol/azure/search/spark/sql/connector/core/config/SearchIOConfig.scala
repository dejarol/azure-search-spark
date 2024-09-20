package com.github.jarol.azure.search.spark.sql.connector.core.config

import com.azure.core.credential.AzureKeyCredential
import com.azure.search.documents.SearchClient
import com.azure.search.documents.indexes.models.{SearchField, SearchIndex}
import com.azure.search.documents.indexes.{SearchIndexClient, SearchIndexClientBuilder}
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import java.util.stream.StreamSupport

/**
 * Parent class for read/write configurations
 * @param localOptions options passed to either a [[org.apache.spark.sql.DataFrameReader]] (when used in [[UsageMode.READ]])
 *                or [[org.apache.spark.sql.DataFrameWriter]] (when used in [[UsageMode.WRITE]])
 * @param globalOptions all options related to the config usage mode, retrieved from the underlying [[SparkConf]] (if any)
 */

class SearchIOConfig(override protected val localOptions: CaseInsensitiveMap[String],
                     override protected val globalOptions: CaseInsensitiveMap[String])
  extends SearchConfig(localOptions, globalOptions)
    with IOConfig {

  /**
   * Alternative constructor
   * @param locals local options
   * @param globals global options
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

  private lazy val searchIndexClient: SearchIndexClient = new SearchIndexClientBuilder()
    .endpoint(getEndpoint)
    .credential(new AzureKeyCredential(getAPIkey))
    .buildClient

  private lazy val searchIndex: SearchIndex = searchIndexClient.getIndex(getIndex)
  private lazy val searchClient: SearchClient = searchIndexClient.getSearchClient(getIndex)

  /**
   * Perform an action using this instance's [[SearchIndexClient]], and get the result
   * @param function action to perform
   * @tparam T action return type
   * @return the action result
   */

  final def withSearchIndexClientDo[T](function: SearchIndexClient => T): T = function.apply(searchIndexClient)

  /**
   * Perform an action using this instance's [[SearchIndex]], and get the result
   * @param function action to perform
   * @tparam T action return type
   * @return the action result
   */

  private final def withSearchIndexDo[T](function: SearchIndex => T): T = function.apply(searchIndex)

  /**
   * Perform an action using this instance's [[SearchClient]], and get the result
   * @param function action to perform
   * @tparam T action return type
   * @return the action result
   */

  final def withSearchClientDo[T](function: SearchClient => T): T = function.apply(searchClient)

  /**
   * Evaluate if this instance's index exists
   * @return true if the index exist
   */

  final def indexExists: Boolean = {

    withSearchIndexClientDo {
      sic =>
        StreamSupport
          .stream(sic.listIndexes().spliterator(), false)
          .anyMatch(i => i.getName.equalsIgnoreCase(getIndex))
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

  protected[config] def allConfigsForMode(sparkConf: SparkConf, mode: UsageMode): Map[String, String] = {

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
