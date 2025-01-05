package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.read.ReadConfig
import org.apache.spark.sql.connector.expressions.filter.Predicate

/**
 * Parent class for Scala-based [[SearchPartitioner]](s)
 * @param readConfig read configuration
 * @param pushedPredicates predicates that support predicate pushdown
 */

abstract class AbstractSearchPartitioner(
                                          protected val readConfig: ReadConfig,
                                          protected val pushedPredicates: Array[Predicate]
                                        )
  extends SearchPartitioner {

}
