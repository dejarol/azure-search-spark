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

    // Infer schema for all non-hidden and selected fields
    SchemaUtils.asStructType(
      filterSearchFields(
        allFields,
        readConfig.select
      )
    )
  }

  /**
   * Filter a collection of Search fields by
   *  - keeping only visible fields
   *  - optionally selecting fields within a selection list
   * @param allFields all index fields
   * @param selection field names to select
   * @return a collection with all visible and selected fields
   */

  protected[schema] def filterSearchFields(allFields: Seq[SearchField], selection: Option[Seq[String]]): Seq[SearchField] = {

    val notHidden: SearchField => Boolean = sf => !sf.isHidden
    val filterPredicate: SearchField => Boolean = selection match {
      case Some(value) => sf => notHidden(sf) && value.contains(sf.getName)
      case None => notHidden
    }

    allFields.filter(filterPredicate)
  }
}
