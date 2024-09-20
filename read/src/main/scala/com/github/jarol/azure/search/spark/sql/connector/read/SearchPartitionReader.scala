package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.models.SearchResult
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input.SparkInternalConverter
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SearchPartition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

import java.util

/**
 * Partition reader for Search dataSource
 * @param readConfig read configuration
 * @param converters map with keys being document keys and values being converters from Search document properties to Spark internal objects
 * @param searchPartition a search partition
 */

class SearchPartitionReader(private val readConfig: ReadConfig,
                            private val converters: Map[String, SparkInternalConverter],
                            private val searchPartition: SearchPartition)
  extends PartitionReader[InternalRow]
    with Logging {

  private lazy val documentConverter: SearchDocumentToInternalRowConverter = SearchDocumentToInternalRowConverter(converters)
  private lazy val searchResultIterator: util.Iterator[SearchResult] = readConfig
    .search(searchPartition.getSearchOptions)
    .iterator()

  override def next(): Boolean = searchResultIterator.hasNext

  override def get(): InternalRow = {

    // Retrieve next document and convert it to an InternalRow
    documentConverter.apply(
      searchResultIterator.next()
        .getDocument(classOf[SearchDocument]
        )
    )
  }

  override def close(): Unit = {

    log.info(s"Closing reader for partition ${searchPartition.getPartitionId}")
  }
}
