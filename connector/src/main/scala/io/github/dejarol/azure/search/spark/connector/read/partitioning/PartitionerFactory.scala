package io.github.dejarol.azure.search.spark.connector.read.partitioning

import io.github.dejarol.azure.search.spark.connector.read.config.ReadConfig

/**
 * Factory for creating an instance of [[SearchPartitioner]].
 * <br>
 * Implementations should take care of defining the logic for creating their own partitioner instance,
 * given the overall [[io.github.dejarol.azure.search.spark.connector.read.config.ReadConfig]] provided by the users.
 * Partitioner options can be accessed through <code>readConfig.partitionerOptions</code>
 */

trait PartitionerFactory {

  /**
   * Create an instance of partitioner
   * @param readConfig overall read configuration provided by the user
   * @return a partitioner instance, to be used for planning input partitions
   */

  def createPartitioner(readConfig: ReadConfig): SearchPartitioner
}
