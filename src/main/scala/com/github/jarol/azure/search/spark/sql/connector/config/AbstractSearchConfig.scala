package com.github.jarol.azure.search.spark.sql.connector.config

import org.apache.spark.sql.SparkSession

abstract class AbstractSearchConfig(protected val options: Map[String, String],
                                    protected val mode: UsageMode)
  extends SearchConfig {

  protected final val configOptions: Map[String, String] = SparkSession.getActiveSession
    .map {
      _.sparkContext.getConf
        .getAllWithPrefix(mode.getPrefix)
        .toMap
    }.getOrElse(Map.empty)

  protected final def safeGet(key: String): Option[String] = {

    options.get(key).orElse {
      configOptions.get(key)
    }
  }

  protected final def unsafeGet[T](key: String, converter: String => T): T = {

    safeGet(key) match {
      case Some(value) => converter(value)
      case None => throw new NoSuchElementException(s"Missing required option $key")
    }
  }

  protected final def safeGetOrDefault[T](key: String, defaultValue: T, converter: String => T): T = {

    safeGet(key) match {
      case Some(value) => converter(value)
      case None => defaultValue
    }
  }

  override def getEndpoint: String = unsafeGet(SearchConfig.END_POINT_CONFIG, identity)

  override def getAPIkey: String = unsafeGet(SearchConfig.API_KEY_CONFIG, identity)

  override def getIndex: String = unsafeGet(SearchConfig.INDEX_CONFIG, identity)
}
