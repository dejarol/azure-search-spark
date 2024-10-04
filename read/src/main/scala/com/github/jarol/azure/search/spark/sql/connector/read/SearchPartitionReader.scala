package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.models.SearchResult
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SearchPartition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

import java.util

/**
 * Partition reader for Search dataSource
 * @param readConfig read configuration
 * @param documentConverter converter from [[SearchDocument]] to [[InternalRow]]
 * @param searchPartition a search partition
 */

class SearchPartitionReader(private val readConfig: ReadConfig,
                            private val documentConverter: SearchDocumentToInternalRowConverter,
                            private val searchPartition: SearchPartition)
  extends PartitionReader[InternalRow]
    with Logging {

  private lazy val searchResultIterator: util.Iterator[SearchResult] = readConfig.withSearchClientDo(searchPartition.getPartitionResults)

  override def next(): Boolean = searchResultIterator.hasNext

  override def get(): InternalRow = {

    // Retrieve next document and convert it to an InternalRow
    documentConverter.apply(
      searchResultIterator.next()
        .getDocument(classOf[SearchDocument])
    )
  }

  override def close(): Unit = {

    log.info(s"Closing reader for partition ${searchPartition.getPartitionId}")
  }
}
