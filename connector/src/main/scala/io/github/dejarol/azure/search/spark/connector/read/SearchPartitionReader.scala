package io.github.dejarol.azure.search.spark.connector.read

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.models.SearchResult
import io.github.dejarol.azure.search.spark.connector.read.config.ReadConfig
import io.github.dejarol.azure.search.spark.connector.read.partitioning.SearchPartition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

import java.util.{Iterator => JIterator}

/**
 * Partition reader for Search dataSource
 * @param readConfig read configuration
 * @param documentEncoder encoder for translating a [[SearchDocument]] into an [[InternalRow]]
 * @param searchPartition a search partition
 */

class SearchPartitionReader(
                             private val readConfig: ReadConfig,
                             private val documentEncoder: SearchDocumentEncoder,
                             private val searchPartition: SearchPartition
                           )
  extends PartitionReader[InternalRow]
    with Logging {

  // Retrieve documents for this partition
  private lazy val searchResultIterator: JIterator[SearchResult] = readConfig.getResultsForPartition(searchPartition)

  override def next(): Boolean = searchResultIterator.hasNext

  override def get(): InternalRow = {

    // Retrieve next document and convert it to an InternalRow
    documentEncoder.apply(
      searchResultIterator.next()
        .getDocument(classOf[SearchDocument])
    )
  }

  override def close(): Unit = {

    log.info(s"Closing reader for partition ${searchPartition.getPartitionId}")
  }
}
