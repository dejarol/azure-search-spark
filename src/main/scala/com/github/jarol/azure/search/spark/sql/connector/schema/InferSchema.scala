package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.{SearchField, SearchIndex}
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.clients.ClientFactory
import com.github.jarol.azure.search.spark.sql.connector.config.{ConfigException, ReadConfig}
import org.apache.spark.sql.types.StructType

/**
 * Object for inferring the schema of an Azure Cognitive Search index
 */

object InferSchema {

  /**
   * Infer the schema by reading options provided to a [[org.apache.spark.sql.DataFrameReader]]
   *
   * @param options options passed to the reader via [[org.apache.spark.sql.DataFrameReader.option]] method
   * @throws InferSchemaException if schema cannot be inferred due to
   *                              - a non existing index
   *                              - a non-retrievable index (i.e. an index whose fields are all hidden)
   * @return the schema of target Search index
   */

  @throws[InferSchemaException]
  def inferSchema(options: Map[String, String]): StructType = {

    // Retrieve all existing indexes
    val readConfig = ReadConfig(options)
    val existingIndexes: Seq[SearchIndex] = JavaScalaConverters.streamToSeq(
      ClientFactory.searchIndexClient(readConfig)
        .listIndexes().stream()
    )

    // Retrieve the requested index (if any)
    val indexName: String = readConfig.getIndex
    existingIndexes.collectFirst {
      case index if index.getName.equalsIgnoreCase(indexName) => index
    } match {

      // If it exists, infer its schema
      case Some(value) => inferSchemaForExistingIndex(
        indexName,
        JavaScalaConverters.listToSeq(value.getFields),
        readConfig.select
      )

      // Otherwise, throw an exception
      case None => throw new InferSchemaException(indexName, "does not exist")
    }
  }

  @throws[InferSchemaException]
  protected[schema] def inferSchemaForExistingIndex(name: String,
                                                    searchFields: Seq[SearchField],
                                                    select: Option[Seq[String]]): StructType = {

    // If there's no retrievable field, throw an exception
    val nonHiddenFields: Seq[SearchField] = searchFields.filterNot(_.isHidden)
    if (nonHiddenFields.isEmpty) {
      throw new InferSchemaException(name, "no retrievable field found")
    } else {
      // Infer schema for all non-hidden and selected fields
      SchemaUtils.asStructType(
        selectFields(
          nonHiddenFields,
          select
        )
      )
    }
  }

  /**
   * Filter a collection of Search fields by
   *  - keeping only visible fields
   *  - optionally selecting fields within a selection list
   * @param allFields all index fields
   * @param selection field names to select
   * @throws ConfigException if none of the selection fields exist in the search index
   * @return a collection with all visible and selected fields
   */

  @throws[ConfigException]
  protected[schema] def selectFields(allFields: Seq[SearchField], selection: Option[Seq[String]]): Seq[SearchField] = {

    selection match {

      // Select only required fields
      case Some(value) =>

        // Fields selected according to given configuration: if empty throw an exception
        val selectedFields: Seq[SearchField] = allFields.filter {
          field => value.contains(field.getName)
        }

        if (selectedFields.isEmpty) {
          throw new ConfigException(
            ReadConfig.SELECT_CONFIG,
            value,
            s"Selected fields (${value.mkString(",")} do not exist"
          )
        } else selectedFields
      case None => allFields
    }
  }
}
