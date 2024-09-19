package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.config.WriteConfig
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.output.SearchMappingSupplier
import org.apache.spark.sql.connector.write.{BatchWrite, Write}
import org.apache.spark.sql.types.StructType

/**
 * Write for Search dataSource
 * @param writeConfig write configuration
 * @param schema schema of input [[org.apache.spark.sql.DataFrame]]
 */

class SearchWrite(
                   private val writeConfig: WriteConfig,
                   private val schema: StructType,
                   private val indexFields: Seq[SearchField]
                 )
  extends Write {

  @throws[SearchWriteException]
  override def toBatch: BatchWrite = {

    // Safely retrieve the converters map
    // If defined, create the action supplier
    // If this latter is defined as well, create the batch write

    SearchMappingSupplier.get(schema, indexFields, writeConfig.getIndex) match {
      case Left(exception) => throw new SearchWriteException(exception)
      case Right(converters) =>

        // Create action supplier
        val indexActionSupplier: IndexActionSupplier = writeConfig.actionColumn.map {
          PerDocumentSupplier.safeApply(_, schema, writeConfig.overallAction) match {
            case Left(cause) => throw new SearchWriteException(cause)
            case Right(supplier) => supplier
          }
        }.getOrElse(
          new ConstantActionSupplier(writeConfig.overallAction)
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
