package com.github.jarol.azure.search.spark.sql.connector

import com.github.jarol.azure.search.spark.sql.connector.core.Constants
import com.github.jarol.azure.search.spark.sql.connector.core.config.IOConfig
import com.github.jarol.azure.search.spark.sql.connector.read.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.write.WriteConfig
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types.StructType

/**
 * Mix-in trait for Search-Spark integration tests
 */

trait SearchSparkSpec
  extends SparkSpec
    with SearchSpec {

  /**
   * Get the minimum set of options required for reading or writing to a Search index
   * @param name index name
   * @return minimum options for read/write operations
   */

  protected final def optionsForAuthAndIndex(name: String): Map[String, String] = {

    Map(
      IOConfig.END_POINT_CONFIG -> SEARCH_END_POINT,
      IOConfig.API_KEY_CONFIG -> SEARCH_API_KEY,
      IOConfig.INDEX_CONFIG -> name
    )
  }

  /**
   * Read data from a target index
   * @param name index name
   * @param filter filter to apply on index data
   * @param select list of fields to select
   * @param schema optional read schema
   * @return index data
   */

  protected final def readIndex(
                                 name: String,
                                 filter: Option[String],
                                 select: Option[Seq[String]],
                                 schema: Option[StructType]
                               ): DataFrame = {

    // Set extra options
    val extraOptions = Map(
      ReadConfig.FILTER_CONFIG -> filter,
      ReadConfig.SELECT_CONFIG -> select.map(_.mkString(","))
    ).collect {
      case (k, Some(v)) => (k, v)
    }

    // Set up a reader
    val reader = spark.read.format(Constants.DATASOURCE_NAME)
      .options(optionsForAuthAndIndex(name))
      .options(extraOptions)

    // Optionally apply schema
    schema match {
      case Some(value) => reader.schema(value).load(name)
      case None => reader.load(name)
    }
  }

  /**
   * Write a dataFrame to a Search index
   * @param df data to write
   * @param name index name
   * @param keyField name of index key field (required)
   * @param extraOptions additional write options
   */

  protected final def writeToIndex(
                                    df: DataFrame,
                                    name: String,
                                    keyField: String,
                                    extraOptions: Option[Map[String, String]]
                                  ): Unit = {

    // Set up the writer
    val basicWriter = df.write.format(Constants.DATASOURCE_NAME)
      .options(optionsForAuthAndIndex(name))
      .option(WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.KEY_FIELD, keyField)

    // Add extra options, if needed
    extraOptions
      .map(basicWriter.options)
      .getOrElse(basicWriter)
      .mode(SaveMode.Append)
      .save()
  }
}
