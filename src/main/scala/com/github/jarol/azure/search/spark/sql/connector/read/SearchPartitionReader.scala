package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.core.util.Context
import com.azure.search.documents.SearchDocument
import com.azure.search.documents.models.SearchResult
import com.github.jarol.azure.search.spark.sql.connector.clients.ClientFactory
import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SearchPartition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

class SearchPartitionReader(private val schema: StructType,
                            private val readConfig: ReadConfig,
                            private val searchPartition: SearchPartition)
  extends PartitionReader[InternalRow] {

  private lazy val documentConverter = DocumentToInternalRowConverter(schema, readConfig)
  private lazy val searchResultIterator: java.util.Iterator[SearchResult] = ClientFactory.searchClient(readConfig)
    .search(null, searchPartition.getSearchOptions, Context.NONE)
    .iterator()

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
