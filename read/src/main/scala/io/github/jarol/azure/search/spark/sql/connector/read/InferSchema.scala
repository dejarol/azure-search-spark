package io.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchField
import io.github.jarol.azure.search.spark.sql.connector.core.schema.SchemaUtils
import org.apache.spark.sql.types.StructType

/**
 * Object for inferring the schema of an Azure Cognitive Search index
 */

object InferSchema {

  /**
   * Infer the schema of an existing Search index
   * @param name index name
   * @param searchFields index search fields
   * @throws InferSchemaException if none of the search fields is retrievable
   * @return the equivalent schema of an index
   */

  @throws[InferSchemaException]
  def forIndex(
                name: String,
                searchFields: Seq[SearchField]
              ): StructType = {

    // If there's no retrievable field, throw an exception
    val nonHiddenFields: Seq[SearchField] = searchFields.filterNot(_.isHidden)
    if (nonHiddenFields.isEmpty) {
      throw InferSchemaException.forIndexWithNoRetrievableFields(name)
    } else {
      // Infer schema for all non-hidden and selected fields
      SchemaUtils.toStructType(nonHiddenFields)
    }
  }
}
