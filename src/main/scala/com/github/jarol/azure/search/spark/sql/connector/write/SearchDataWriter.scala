package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.IndexDocumentsBatch
import com.azure.search.documents.models.{IndexAction, IndexActionType}
import com.github.jarol.azure.search.spark.sql.connector.config.WriteConfig
import com.github.jarol.azure.search.spark.sql.connector.utils.Generics
import com.github.jarol.azure.search.spark.sql.connector.{AzureSparkException, JavaScalaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.{DataTypes, StructType}

class SearchDataWriter(private val writeConfig: WriteConfig,
                       private val schema: StructType)
  extends DataWriter[InternalRow] {

  private lazy val batchSize: Int = writeConfig.batchSize
  private var actionsBatch: Seq[IndexAction[SearchDocument]] = Seq.empty

  override def write(record: InternalRow): Unit = {

    val actionType: IndexActionType = writeConfig.actionColumn match {
      case Some(value) => SearchDataWriter.actionForDocument(record, schema, value)
      case None => writeConfig.action.getOrElse(IndexActionType.MERGE_OR_UPLOAD)
    }

    val document: SearchDocument = null
    val indexAction: IndexAction[SearchDocument] = new IndexAction[SearchDocument]
      .setDocument(document)
      .setActionType(actionType)

    actionsBatch = actionsBatch :+ indexAction
    if (actionsBatch.size.equals(batchSize)) {
      val documentsBatch = new IndexDocumentsBatch[SearchDocument]
        .addActions(
          JavaScalaConverters.seqToList(documentsBatch)
        )
    }
  }

  override def commit(): WriterCommitMessage = ???

  override def abort(): Unit = ???

  override def close(): Unit = ???
}

object SearchDataWriter {

  protected[write] def actionForDocument(row: InternalRow, schema: StructType, name: String): IndexActionType = {

    val indexOfActionColumn: Int = schema.zipWithIndex.collectFirst {
      case (field, i) if field.name.equalsIgnoreCase(name) &&
        field.dataType.equals(DataTypes.StringType) => i
    }.getOrElse {
      throw new AzureSparkException(
        s"Action column $name could not be found or it's not a string column"
      )
    }

    if (row.isNullAt(indexOfActionColumn)) {
      IndexActionType.MERGE_OR_UPLOAD
    } else {
      Generics.safeValueOfEnum[IndexActionType](
        row.getString(indexOfActionColumn),
        (e, s) => e.name().equalsIgnoreCase(s) || e.toString.equalsIgnoreCase(s)
      ).getOrElse(
        IndexActionType.MERGE_OR_UPLOAD
      )
    }
  }
}
