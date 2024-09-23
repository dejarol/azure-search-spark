package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.models.IndexActionType
import org.apache.spark.sql.catalyst.InternalRow

/**
 * Supplier that provides a constant value for all [[InternalRow]](s)
 * @param default default value
 */

case class ConstantActionSupplier(private val default: IndexActionType)
  extends IndexActionSupplier {

  override def get(row: InternalRow): IndexActionType = default
}
