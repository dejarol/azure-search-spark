package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.clients.ClientFactory
import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import org.apache.spark.sql.types.StructType

/**
 * Object for inferring the schema of an Azure Cognitive Search index
 */

object InferSchema {

  /**
   * Infer the schema by reading options provided to a [[org.apache.spark.sql.DataFrameReader]]
   * @param options options passed to the reader via [[org.apache.spark.sql.DataFrameReader.option()]] method
   * @return the schema of target Search index
   */

  def inferSchema(options: Map[String, String]): StructType = {

    // Retrieve all index fields
   val readConfig = ReadConfig(options)
    val allFields: Seq[SearchField] = JavaScalaConverters.listToSeq(
      ClientFactory.indexClient(readConfig)
        .getIndex(readConfig.getIndex)
        .getFields
    )

    // Set up a predicate for selecting a field, depending on provided select fields
    val shouldBeSelected: SearchField => Boolean = readConfig.select match {
      case Some(value) => sf => value.contains(sf.getName)
      case None => _ => true
    }

    // Infer schema for all non-hidden and selected fields
    SchemaUtils.asStructType(
      allFields.filter {
        sf => !sf.isHidden && shouldBeSelected(sf)
      }
    )
  }
}
