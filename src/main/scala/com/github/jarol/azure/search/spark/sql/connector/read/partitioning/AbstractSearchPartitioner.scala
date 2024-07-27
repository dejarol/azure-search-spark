package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig

abstract class AbstractSearchPartitioner(protected val readConfig: ReadConfig)
  extends SearchPartitioner {

}
