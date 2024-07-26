package com.github.jarol.azure.search.spark.sql.connector.config

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Parent class for all Search configurations
 * @param options options passed to either a [[org.apache.spark.sql.DataFrameReader]] (when used in [[UsageMode.READ]])
 *                or [[org.apache.spark.sql.DataFrameWriter]] (when used in [[UsageMode.WRITE]])
 * @param configOptions all options related to the config usage mode, retrieved from the underlying [[SparkConf]] (if any)
 * @param usageMode usage mode
 */

abstract class AbstractSearchConfig(protected val options: Map[String, String],
                                    protected val configOptions: Map[String, String],
                                    protected val usageMode: UsageMode)
  extends SearchConfig {

  /**
   * Safely get the value of a key by inspecting first provided options and then [[org.apache.spark.SparkConf]] options
   * @param key key
   * @return the optional value related to given key
   */

  protected final def safelyGet(key: String): Option[String] = {

    options.get(key).orElse {
      configOptions.get(key)
    }
  }

  /**
   * Get the value of a key and transform it, or throw an exception if not found
   * @param key key
   * @param converter value converter
   * @throws NoSuchElementException if the key is not found
   * @tparam T output value type
   * @return the value related to given key, converted using given converter
   */

  @throws[NoSuchElementException]
  protected final def unsafelyGet[T](key: String, converter: String => T): T = {

    safelyGet(key) match {
      case Some(value) => converter(value)
      case None => throw new NoSuchElementException(s"Missing required option $key")
    }
  }

  protected final def getOrDefault[T](key: String, converter: String => T, defaultValue: T): T = {

    safelyGet(key) match {
      case Some(value) => converter(value)
      case None => defaultValue
    }
  }

  override def getEndpoint: String = unsafelyGet(SearchConfig.END_POINT_CONFIG, identity)

  override def getAPIkey: String = unsafelyGet(SearchConfig.API_KEY_CONFIG, identity)

  override def getIndex: String = unsafelyGet(SearchConfig.INDEX_CONFIG, identity)
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
