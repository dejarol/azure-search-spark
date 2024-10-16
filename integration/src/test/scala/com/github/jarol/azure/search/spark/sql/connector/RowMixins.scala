package com.github.jarol.azure.search.spark.sql.connector

import org.apache.spark.sql.Row

import scala.language.implicitConversions

/**
 * Mix-in trait for dealing with Spark [[Row]]
 */

trait RowMixins {

  import RowMixins.RowExtensions

  /**
   * Implicit conversion from a row to its extension
   * @param row row
   * @return a row extension
   */

  protected final implicit def toRowExtensions(row: Row): RowExtensions = new RowExtensions(row)
}

object RowMixins {

  /**
   * Extension methods for Spark rows
   * @param row row
   */

  class RowExtensions(private val row: Row) {

    /**
     * Gets the value of a column as an Option (defined if the underlying value is not null)
     * @param name column name
     * @tparam T target type
     * @return an empty Option for null values, a non-empty Option for non-null values
     */

    final def getAsOpt[T](name: String): Option[T] = {

      if (row.isNullAt(row.fieldIndex(name))) {
        None
      } else {
        Some(row.getAs[T](name))
      }
    }
  }
}
