package io.github.dejarol.azure.search.spark.connector.read.partitioning

import io.github.dejarol.azure.search.spark.connector.read.config.ReadConfig

/**
 * Parent class for Scala-based [[SearchPartitioner]](s)
 *
 * @param readConfig read configuration
 */

abstract class AbstractSearchPartitioner(protected val readConfig: ReadConfig)
  extends SearchPartitioner {

  // Partitioner options
  protected final lazy val partitionerOptions: SearchConfig = readConfig.partitionerOptions
}
