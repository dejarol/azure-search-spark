package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException
import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SearchPartition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

class SearchPartitionReaderFactory(private val schema: StructType,
                                   private val readConfig: ReadConfig)
  extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {

    partition match {
      case sp: SearchPartition => new SearchPartitionReader(readConfig, sp)
      case _ => throw new AzureSparkException(s"Found a partition of type ${partition.getClass.getName}, " +
        s"expecting a ${classOf[SearchPartition].getName}")
    }
  }
}
