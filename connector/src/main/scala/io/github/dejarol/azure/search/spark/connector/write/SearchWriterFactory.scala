package io.github.dejarol.azure.search.spark.connector.write

import io.github.dejarol.azure.search.spark.connector.core.schema.conversion.SchemaViolationException
import io.github.dejarol.azure.search.spark.connector.write.config.WriteConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * [[org.apache.spark.sql.connector.write.DataWriterFactory]] implementation for this dataSource
 * @param writeConfig write configuration
 * @param schema Dataframe schema
 */

class SearchWriterFactory(
                           private val writeConfig: WriteConfig,
                           private val schema: StructType,
                         )
  extends DataWriterFactory {

  /**
   * Creates the [[org.apache.spark.sql.connector.write.DataWriter]] implementation of this datasource
   * @param partitionId partition id
   * @param taskId task id
   * @return the data writer implementation of this datasource
   */

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
