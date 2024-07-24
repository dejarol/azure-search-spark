package com.github.jarol.azure.search.spark.sql.connector.read

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

class SearchPartitionReaderFactory() extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = new SearchPartitionReader()
}
