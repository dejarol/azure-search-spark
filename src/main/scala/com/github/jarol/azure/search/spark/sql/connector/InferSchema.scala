package com.github.jarol.azure.search.spark.sql.connector

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.core.config.ConfigException
import com.github.jarol.azure.search.spark.sql.connector.core.schema.SchemaUtils
import com.github.jarol.azure.search.spark.sql.connector.read.ReadConfig
import org.apache.spark.sql.types.StructType

/**
 * Object for inferring the schema of an Azure Cognitive Search index
 */

object InferSchema {

  /**
   * Infer the schema of an existing Search index
   * @param name index name
   * @param searchFields index search fields
   * @param select fields to select (specified by the user)
   * @throws InferSchemaException if none of the search fields is retrievable
   * @return the equivalent schema of an index
   */

  @throws[InferSchemaException]
  def forIndex(
                           name: String,
                           searchFields: Seq[SearchField],
                           select: Option[Seq[String]]
                         ): StructType = {

    // If there's no retrievable field, throw an exception
    val nonHiddenFields: Seq[SearchField] = searchFields.filterNot(_.isHidden)
    if (nonHiddenFields.isEmpty) {
      throw InferSchemaException.forIndexWithNoRetrievableFields(name)
    } else {
      // Infer schema for all non-hidden and selected fields
      SchemaUtils.toStructType(
        selectFields(
          nonHiddenFields,
          select
        )
      )
    }
  }

  /**
   * Filter a collection of Search fields by selecting fields within a selection list
   * @param allFields all index fields
   * @param selection field names to select
   * @throws ConfigException if none of the selection fields exist in the search index
   * @return a collection with all visible and selected fields
   */

  @throws[ConfigException]
  protected[connector] def selectFields(allFields: Seq[SearchField], selection: Option[Seq[String]]): Seq[SearchField] = {

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
