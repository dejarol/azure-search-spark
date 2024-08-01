package com.github.jarol.azure.search.spark.sql.connector.write

import com.github.jarol.azure.search.spark.sql.connector.config.WriteConfig
import org.apache.spark.sql.connector.write.{Write, WriteBuilder}

class SearchWriteBuilder(private val writeConfig: WriteConfig)
  extends WriteBuilder {

  override def build(): Write = {

    new SearchWrite(writeConfig)
  }
}
