package com.github.jarol.azure.search.spark.sql.connector

import com.github.jarol.azure.search.spark.sql.connector.core.config.IOConfig

trait SearchSparkSpec
  extends SparkSpec
    with SearchSpec {

  protected final def optionsForAuthAndIndex(name: String): Map[String, String] = {

    Map(
      IOConfig.END_POINT_CONFIG -> SEARCH_END_POINT,
      IOConfig.API_KEY_CONFIG -> SEARCH_API_KEY,
      IOConfig.INDEX_CONFIG -> name
    )
  }
}
