package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import org.apache.spark.sql.types.StructType

trait CodecFactory[CType, InternalCType] {

  final def safeApply(
                       schema: StructType,
                       searchFields: Seq[SearchField]
                     ): Either[SchemaViolationException, CType] = {

    getInternalMapping(schema, searchFields)
      .left.map {
        v => new SchemaViolationException(
          JavaScalaConverters.seqToList(v)
        )
      }.right.map(toCodec)
  }

  protected def getInternalMapping(
                                    schema: StructType,
                                    searchFields: Seq[SearchField]
                                  ): Either[Seq[SchemaViolation], Map[SearchIndexColumn, InternalCType]]

  protected def toCodec(internal: Map[SearchIndexColumn, InternalCType]): CType
}
