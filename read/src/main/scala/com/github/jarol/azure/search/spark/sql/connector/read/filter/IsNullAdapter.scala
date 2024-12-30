package com.github.jarol.azure.search.spark.sql.connector.read.filter

/**
 * Adapter for null equality OData filters
 * @param fieldNames field names. It will be a single element for a 0-level (or top-level) field, a collection of N-1 elements for an N-th level nested field
 * @param negate true for creating a filter that evaluates non-null equality
 */

private[filter] case class IsNullAdapter(
                                          private val fieldNames: Seq[String],
                                          private val negate: Boolean
                                        )
  extends V2ExpressionAdapter {

  override def getODataFilter: String = {

    val fieldName = fieldNames.mkString("/")
    val operator = if (negate) "ne" else "eq"
    s"$fieldName $operator null"
  }
}
