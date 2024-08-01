package com.github.jarol.azure.search.spark.sql.connector.write

import com.github.jarol.azure.search.spark.sql.connector.config.WriteConfig
import org.apache.spark.sql.connector.write.{BatchWrite, Write}

class SearchWrite(private val writeConfig: WriteConfig)
  extends Write {

  override def toBatch: BatchWrite = {

    new SearchBatchWrite()
  }
}
