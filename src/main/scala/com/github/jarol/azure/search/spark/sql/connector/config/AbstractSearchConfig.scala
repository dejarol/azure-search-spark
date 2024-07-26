package com.github.jarol.azure.search.spark.sql.connector.config

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

/**
 * Parent class for all Search configurations
 * @param options options passed to either a [[org.apache.spark.sql.DataFrameReader]] (when used in [[UsageMode.READ]])
 *                or [[org.apache.spark.sql.DataFrameWriter]] (when used in [[UsageMode.WRITE]])
 * @param sparkConfOptions all options related to the config usage mode, retrieved from the underlying [[SparkConf]] (if any)
 * @param usageMode usage mode
 */

abstract class AbstractSearchConfig(protected val options: Map[String, String],
                                    protected val sparkConfOptions: Map[String, String],
                                    protected val usageMode: UsageMode)
  extends SearchConfig {

  /**
   * Safely get the value of a key by inspecting first provided options and then [[org.apache.spark.SparkConf]] options
   * @param key key
   * @return the optional value related to given key
   */

  protected[config] final def safelyGet(key: String): Option[String] = {

    options.get(key).orElse {
      sparkConfOptions.get(key)
    }
  }

  /**
   * Get the value of a key and transform it, or throw an exception if not found
   * @param key key
   * @throws ConfigException if the key is not found
   * @return the value related to given key, converted using given converter
   */

  @throws[ConfigException]
  protected[config] final def unsafelyGet(key: String): String = {

    safelyGet(key) match {
      case Some(value) => value
      case None => throw ConfigException.missingKey(key)
    }
  }

  /**
   * Get the value of a key, or a default
   * @param key key
   * @param defaultValue default value
   * @return the value key or the default if the key is missing
   */

  protected[config] final def getOrDefault(key: String, defaultValue: String): String = safelyGet(key).getOrElse(defaultValue)

  /**
   * Get the value of a key and map its value using a conversion function, or get a default value
   * @param key key
   * @param defaultValue default value to return in case of missing key
   * @param conversion value conversion function
   * @tparam T conversion type
   * @return the default instance if key is missing, or the converted value
   */

  protected[config] final def getAsOrDefault[T](key: String, defaultValue: T, conversion: String => T): T = {

    safelyGet(key).map {
      value => Try {
        conversion(value)
      } match {
        case Failure(exception) => throw new ConfigException(key, value, exception)
        case Success(value) => value
      }
    }.getOrElse(defaultValue)
  }

  override def getEndpoint: String = unsafelyGet(SearchConfig.END_POINT_CONFIG)

  override def getAPIkey: String = unsafelyGet(SearchConfig.API_KEY_CONFIG)

  override def getIndex: String = unsafelyGet(SearchConfig.INDEX_CONFIG)
}

object AbstractSearchConfig {

  /**
   * Extract all options from given SparkConf that starts with the prefix of a usage mode
   * @param sparkConf an instance of [[SparkConf]]
   * @param usageMode usage mode
   * @return all key-value pairs whose keys start with given mode prefix
   */

  protected[config] def allConfigsForMode(sparkConf: SparkConf, usageMode: UsageMode): Map[String, String] = {

   sparkConf
      .getAllWithPrefix(usageMode.prefix())
      .toMap
  }

  /**
   * Retrieve all options related to a mode from the active SparkSession (if any)
   * @param usageMode usage mode
   * @return an empty Map if no [[SparkSession]] is active, all options related to the mode otherwise
   */

  protected[config] def allConfigsFromActiveSessionForMode(usageMode: UsageMode): Map[String, String] = {

    SparkSession.getActiveSession match {
      case Some(value) => allConfigsForMode(value.sparkContext.getConf, usageMode)
      case None => Map.empty
    }
  }
}
