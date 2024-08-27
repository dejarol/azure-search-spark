package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

class SearchScan(private val schema: StructType,
                 private val readConfig: ReadConfig)
  extends Scan {

  override def readSchema(): StructType = schema

  override def toBatch: Batch = {

    new SearchBatch(schema, readConfig)
  }
}

object SearchScan {

  def apply(): SearchScan = {

    new SearchScan(
      StructType(
        Seq.empty
      ),
      null
    )
  }
}
