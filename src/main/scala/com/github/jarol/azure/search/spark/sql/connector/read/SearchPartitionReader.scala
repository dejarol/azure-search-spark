package com.github.jarol.azure.search.spark.sql.connector.read

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

class SearchPartitionReader()
  extends PartitionReader[InternalRow] {

  override def next(): Boolean = ???

  override def get(): InternalRow = ???

  override def close(): Unit = ???
}
