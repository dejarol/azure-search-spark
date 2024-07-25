package com.github.jarol.azure.search.spark.sql.connector.config

import org.apache.spark.sql.SparkSession

/**
 * Parent class for all Search configurations
 * @param options options passed to either a [[org.apache.spark.sql.DataFrameReader]] (when used in [[UsageMode.READ]])
 *                or [[org.apache.spark.sql.DataFrameWriter]] (when used in [[UsageMode.WRITE]])
 * @param mode usage mode
 */

abstract class AbstractSearchConfig(protected val options: Map[String, String],
                                    protected val mode: UsageMode)
  extends SearchConfig {

  /**
   * Options retrieved by the underlying [[org.apache.spark.SparkConf]] (if any)
   */

  protected final val configOptions: Map[String, String] = SparkSession.getActiveSession
    .map {
      _.sparkContext.getConf
        .getAllWithPrefix(mode.prefix)
        .toMap
    }.getOrElse(Map.empty)

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

  override def getEndpoint: String = unsafelyGet(SearchConfig.END_POINT_CONFIG, identity)

  override def getAPIkey: String = unsafelyGet(SearchConfig.API_KEY_CONFIG, identity)

  override def getIndex: String = unsafelyGet(SearchConfig.INDEX_CONFIG, identity)
}
