package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.config.WriteConfig
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

  override def toBatch: BatchWrite = {

    val indexActionTypeGetter: Option[IndexActionTypeGetter] = writeConfig.actionColumn.map {
      IndexActionTypeGetter(_, schema, writeConfig.overallAction)
    }

    SearchBatchWrite.safeApply(schema, indexFields) match {
      case Left(value) => ???
      case Right(value) => value
    }
  }
}
