package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchField
import org.apache.spark.sql.types.StructType

trait SchemaCompatibilityCheck {

  def maybeException: Option[SchemaCompatibilityException]
}

abstract class SchemaCompatibilityCheckImpl[T](protected val schema: StructType,
                                               protected val searchFields: Seq[SearchField],
                                               protected val index: String)
  extends SchemaCompatibilityCheck {

  override final def maybeException: Option[SchemaCompatibilityException] = {

    val result: Set[T] = computeResult
    if (result.nonEmpty) {
      Some(
        new SchemaCompatibilityException(
          exceptionMessage(result)
        )
      )
    } else {
      None
    }
  }

  protected def computeResult: Set[T]

  protected def exceptionMessage(result: Set[T]): String
}
