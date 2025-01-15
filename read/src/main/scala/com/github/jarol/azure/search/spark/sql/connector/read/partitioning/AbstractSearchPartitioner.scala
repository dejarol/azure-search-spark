package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.read.config.ReadConfig

/**
 * Parent class for Scala-based [[SearchPartitioner]](s)
 *
 * @param readConfig read configuration
 */

abstract class AbstractSearchPartitioner(protected val readConfig: ReadConfig)
  extends SearchPartitioner {

}
