package com.github.jarol.azure.search.spark.sql.connector.core.config

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import java.util.function.Supplier
import scala.util.Try

/**
 * Parent class for all Search configurations
 * @param dsOptions options passed to the dataSource
 */

class SearchConfig(protected val dsOptions: CaseInsensitiveMap[String])
  extends Serializable {

  /**
   * Create an instance from a simple map
   * @param dsOptions local options
   */

  def this(dsOptions: Map[String, String]) = {

    this(
      CaseInsensitiveMap(dsOptions)
    )
  }

  /**
   * Safely get the value of a key by inspecting local options and then [[org.apache.spark.SparkConf]] options
   * @param key key
   * @return the optional value related to given key
   */

  final def get(key: String): Option[String] = dsOptions.get(key)

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
   * @param prefix optional key prefix
   * @param supplier optional message supplier for better exception explanation
   * @throws ConfigException if the key is not found
   * @return the value related to given key
   */

  @throws[ConfigException]
  final def unsafelyGet(
                         key: String,
                         prefix: Option[String],
                         supplier: Option[Supplier[String]]
                       ): String = {

    get(key) match {
      case Some(value) => value
      case None => throw SearchConfig.exceptionForMissingOption(key, prefix, supplier)
    }
  }

  /**
   * Get the value of a key as a typed value by applying a conversion
   * @param key key
   * @param conversion conversion function
   * @param prefix optional key prefix
   * @param supplier optional message supplier
   * @throws ConfigException if the key is not found or the conversion fails
   * @return the value related to given key, transformed according to the given conversion
   */

  @throws[ConfigException]
  final def unsafelyGetAs[T](
                              key: String,
                              conversion: String => T,
                              prefix: Option[String],
                              supplier: Option[Supplier[String]]
                            ): T = {

    SearchConfig.convertOrThrow[T](
      key,
      unsafelyGet(key, prefix, supplier),
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
  protected[config] final def getOrDefaultAs[T](
                                                 key: String,
                                                 defaultValue: T,
                                                 conversion: String => T
                                               ): T = {

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
      SearchConfig.allWithPrefix(dsOptions, prefix)
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
   * Create a [[ConfigException]] for a missing key
   * @param key key
   * @param prefix key prefix
   * @param supplier message supplier
   * @return an exception instance
   */

  private[config] def exceptionForMissingOption(
                                                 key: String,
                                                 prefix: Option[String],
                                                 supplier: Option[Supplier[String]]
                                               ): ConfigException = {

    ConfigException.forMissingOption(
      key,
      prefix.orNull,
      supplier.orNull
    )
  }

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
      case Left(exception) => throw ConfigException.forIllegalOptionValue(key, value, exception)
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
