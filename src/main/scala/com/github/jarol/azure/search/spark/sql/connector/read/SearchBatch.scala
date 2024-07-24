package com.github.jarol.azure.search.spark.sql.connector.read

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}

class SearchBatch()
  extends Batch {

  override def planInputPartitions(): Array[InputPartition] = ???

  override def createReaderFactory(): PartitionReaderFactory = new SearchPartitionReaderFactory()
}
