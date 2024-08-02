package com.github.jarol.azure.search.spark.sql.connector.write

import com.github.jarol.azure.search.spark.sql.connector.config.WriteConfig
import org.apache.spark.sql.connector.write.{Write, WriteBuilder}
import org.apache.spark.sql.types.StructType

class SearchWriteBuilder(private val writeConfig: WriteConfig,
                         private val schema: StructType)
  extends WriteBuilder {

  override def build(): Write = {

    new SearchWrite(
      writeConfig,
      schema
    )
  }
}
