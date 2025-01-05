package com.github.jarol.azure.search.spark.sql.connector.read

import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.filter.Predicate

/**
 * Mix-in trait for specs that deal with [[Predicate]]
 */

trait V2PredicateFactory {

  /**
   * Create a <code>IS_NULL</code> or <code>IS_NOT_NULL</code> predicate on a top-level field
   * @param fieldName field name
   * @param negate true for creating <code>IS_NOT_NULL</code> predicates
   * @return a null equality predicate
   */

  protected final def createNullEqualityPredicate(
                                                   fieldName: String,
                                                   negate: Boolean
                                                 ): Predicate = {

    new Predicate(
      if (negate) "IS_NOT_NULL" else "IS_NULL",
      Array(
        new NamedReference {
          override def fieldNames(): Array[String] = Array(fieldName)
        }
      )
    )
  }
}
