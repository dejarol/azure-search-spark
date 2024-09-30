package com.github.jarol.azure.search.spark.sql.connector.core.config

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import scala.util.Try

/**
 * Parent class for all Search configurations
 * @param localOptions options passed to the dataSource
 * @param globalOptions options retrieved from the underlying Spark configuration
 */

class SearchConfig(protected val localOptions: CaseInsensitiveMap[String],
                   protected val globalOptions: CaseInsensitiveMap[String])
  extends Serializable {

  /**
   * Secondary constructor
   * @param local local options
   * @param global global options
   */

  def this(local: Map[String, String], global: Map[String, String]) = {

    this(
      CaseInsensitiveMap(local),
      CaseInsensitiveMap(global)
    )
  }

  /**
   * Whether this config is empty or not (true only if both local and global options are empty)
   * @return config emptiness flag
   */

  final def isEmpty: Boolean = localOptions.isEmpty && globalOptions.isEmpty

  /**
   * Safely get the value of a key by inspecting local options and then [[org.apache.spark.SparkConf]] options
   * @param key key
   * @return the optional value related to given key
   */

  final def get(key: String): Option[String] = {

    localOptions.get(key)
      .orElse(globalOptions.get(key))
  }

  /**
   * Safely get a typed value for a key
   * @param key key
   * @param conversion conversion function
   * @tparam T conversion target type
   * @throws ConfigException if a value for given key exists, but its conversion fails
   * @return an empty value if the key does not exist, a non-empty value otherwise
   */

  @throws[ConfigException]
  final def getAs[T](key: String, conversion: String => T): Option[T] = {

    get(key).map {
      SearchConfig.convertOrThrow[T](key, _, conversion)
    }
  }

  /**
   * Get the value of a key, or throw an exception if not found
   * @param key key
   * @throws ConfigException if the key is not found
   * @return the value related to given key, converted using given converter
   */

  @throws[ConfigException]
  final def unsafelyGet(key: String): String = {

    get(key) match {
      case Some(value) => value
      case None => throw new ConfigException(s"Missing required option $key")
    }
  }

  /**
   * Get the value of a key, or throw an exception if not found
   * @param key key
   * @throws ConfigException if the key is not found
   * @return the value related to given key, converted using given converter
   */

  @throws[ConfigException]
  final def unsafelyGetAs[T](key: String, conversion: String => T): T = {

    SearchConfig.convertOrThrow[T](
      key,
      unsafelyGet(key),
      conversion
    )
  }

  /**
   * Get the value of a key, or a default
   * @param key key
   * @param defaultValue default value
   * @return the value key or the default if the key is missing
   */

  protected[config] final def getOrDefault(key: String, defaultValue: String): String = get(key).getOrElse(defaultValue)

  /**
   * Get the value of a key and map its value using a conversion function, or get a default value
   * @param key key
   * @param defaultValue default value to return in case of missing key
   * @param conversion value conversion function
   * @tparam T target conversion type
   * @throws ConfigException if a value for given key exists, but its conversion fails
   * @return the default instance if key is missing, or the converted value
   */

  @throws[ConfigException]
  protected[config] final def getOrDefaultAs[T](key: String, defaultValue: T, conversion: String => T): T = {

    get(key).map {
      SearchConfig.convertOrThrow[T](key, _, conversion)
    }.getOrElse(defaultValue)
  }

  /**
   * Create a new config that collects all options that start with given prefix
   * @param prefix key prefix
   * @return a new instance of [[SearchConfig]]
   */

  final def getAllWithPrefix(prefix: String): SearchConfig = {

    new SearchConfig(
      SearchConfig.allWithPrefix(localOptions, prefix),
      SearchConfig.allWithPrefix(globalOptions, prefix)
    )
  }

  /**
   * Get the value of a key as list of non-empty strings
   * @param key key
   * @return a non-empty collection of strings if the original value is not blank
   */

  final def getAsList(key: String): Option[Seq[String]] = {

    get(key).flatMap {
      v =>
        if (StringUtils.isBlank(v)) {
          None
        } else {
          Some(v.split(",").map(_.trim))
        }
    }
  }
}

object SearchConfig {

  /**
   * Convert the value related to a key, throwing a [[ConfigException]] if conversion fails
   * @param key key
   * @param value value to convert
   * @param conversion conversion function
   * @tparam T conversion target type
   * @throws ConfigException if conversion fails
   * @return the converted version of input value
   */

  @throws[ConfigException]
  private[config] final def convertOrThrow[T](key: String, value: String, conversion: String => T): T = {

    Try {
      conversion(value)
    }.toEither match {
      case Left(exception) => throw new ConfigException(key, value, exception)
      case Right(value) => value
    }
  }

  /**
   * Filter the entries of a map by taking only those whose keys start with given prefix, and return them
   * as a new map same values but with prefix-stripped keys
   * @param original original map
   * @param prefix key prefix
   * @tparam V value type
   * @return a new map with matching entries, with prefix-stripped keys
   */

  private[config] def allWithPrefix[V](original: Map[String, V], prefix: String): Map[String, V] = {

    val lowerPrefix = prefix.toLowerCase
    original.collect {
      case (k, v) if k.toLowerCase.startsWith(lowerPrefix) =>
        (k.toLowerCase.stripPrefix(lowerPrefix), v)
    }
  }
}
