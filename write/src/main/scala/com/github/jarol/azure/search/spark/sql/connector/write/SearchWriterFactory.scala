package com.github.jarol.azure.search.spark.sql.connector.write

import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.SchemaViolationException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * [[DataWriterFactory]] implementation for Search dataSource
 * @param writeConfig write configuration
 * @param schema Dataframe schema
 */

class SearchWriterFactory(
                           private val writeConfig: WriteConfig,
                           private val schema: StructType,
                         )
  extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {

    new SearchDataWriter(
      writeConfig,
      createDocumentDecoder(),
      createIndexActionSupplier(),
      partitionId,
      taskId
    )
  }

  /**
   * Create the [[IndexActionSupplier]]
   * @throws IllegalIndexActionTypeColumnException if the specified action type column is not eligible (either non-existing or non-string)
   * @return a index action supplier instance
   */

  @throws[IllegalIndexActionTypeColumnException]
  private def createIndexActionSupplier(): IndexActionSupplier = {

    // Create action supplier
    writeConfig.actionColumn.map {
      PerDocumentSupplier.safeApply(_, schema, writeConfig.overallAction) match {
        case Left(value) => throw value
        case Right(supplier) => supplier
      }
    }.getOrElse(
      IndexActionSupplier.createConstantSupplier(
        writeConfig.overallAction
      )
    )
  }

  /**
   * Create a decoder for converting Spark internal rows to Search documents
   * @throws SchemaViolationException if the decoder cannot be built
   * @return a decoder from Spark internal rows to Search documents
   */

  @throws[SchemaViolationException]
  private def createDocumentDecoder(): SearchDocumentDecoder = {

    // Exclude index action column from mapping, if defined
    val schemaMaybeWithoutActionColumn: Seq[StructField] = writeConfig.actionColumn match {
      case Some(value) => schema.filterNot {
        _.name.equalsIgnoreCase(value)
      }
      case None => schema
    }

    SearchDocumentDecoder.safeApply(
      StructType(schemaMaybeWithoutActionColumn),
      writeConfig.getSearchIndexFields
    ) match {
      case Left(value) => throw value
      case Right(value) => value
    }
  }
}
