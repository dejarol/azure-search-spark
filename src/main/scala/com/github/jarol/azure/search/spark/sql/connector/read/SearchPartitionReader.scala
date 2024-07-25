package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.models.SearchResult
import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

import java.util.Collections

class SearchPartitionReader(private val schema: StructType,
                            private val readConfig: ReadConfig)
  extends PartitionReader[InternalRow] {

  private val documentConverter = DocumentToInternalRowConverter(schema, readConfig)
  private val searchResultIterator: java.util.Iterator[SearchResult] = Collections.emptyIterator()

  override def next(): Boolean = searchResultIterator.hasNext

  override def get(): InternalRow = {

    documentConverter(
      searchResultIterator.next()
        .getDocument(classOf[SearchDocument])
    )
  }

  override def close(): Unit = {

  }
}
