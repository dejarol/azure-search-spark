package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.models.SearchResult
import com.github.jarol.azure.search.spark.sql.connector.clients.ClientFactory
import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SearchPartition
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.SparkInternalConverter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

import java.util

class SearchPartitionReader(private val readConfig: ReadConfig,
                            private val converters: Map[String, SparkInternalConverter],
                            private val searchPartition: SearchPartition)
  extends PartitionReader[InternalRow] {

  private lazy val documentConverter: SearchDocumentToInternalRowConverter = SearchDocumentToInternalRowConverter(converters)
  private lazy val searchResultIterator: util.Iterator[SearchResult] = ClientFactory
    .doSearch(readConfig, searchPartition.getSearchOptions)
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
