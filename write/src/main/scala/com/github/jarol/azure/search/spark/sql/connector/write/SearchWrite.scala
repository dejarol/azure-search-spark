package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchField
import org.apache.spark.sql.connector.write.{BatchWrite, Write}
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * [[Write]] implementation for Search dataSource
 * @param writeConfig write configuration
 * @param schema schema of input [[org.apache.spark.sql.DataFrame]]
 */

class SearchWrite(
                   private val writeConfig: WriteConfig,
                   private val schema: StructType,
                   private val indexFields: Seq[SearchField]
                 )
  extends Write {

  /**
   * Create the BatchWrite for this dataSource
   * @throws SearchWriteException if either
   *                              - the specified index action column is invalid
   *                              - there are some incompatibilities between Dataframe schema and target index schema
   * @return the BatchWrite for this dataSource
   */

  @throws[SearchWriteException]
  override def toBatch: BatchWrite = {

    // Exclude index action column from mapping, if defined
    val schemaToMap: Seq[StructField] = writeConfig.actionColumn match {
      case Some(value) => schema.filterNot {
        _.name.equalsIgnoreCase(value)
      }
      case None => schema
    }

    // Safely retrieve the converters map
    // If defined, create the action supplier
    // If this latter is defined as well, create the batch write

    WriteMappingSupplier.get(schemaToMap, indexFields, writeConfig.getIndex) match {
      case Left(exception) => throw new SearchWriteException(exception)
      case Right(converters) =>

        // Create action supplier
        val indexActionSupplier: IndexActionSupplier = writeConfig.actionColumn.map {
          PerDocumentSupplier.safeApply(_, schema, writeConfig.overallAction) match {
            case Left(cause) => throw new SearchWriteException(cause)
            case Right(supplier) => supplier
          }
        }.getOrElse(
          ConstantActionSupplier(writeConfig.overallAction)
        )

        // Create batch write
        new SearchBatchWrite(
          writeConfig,
          converters,
          indexActionSupplier
        )
    }
  }
}
