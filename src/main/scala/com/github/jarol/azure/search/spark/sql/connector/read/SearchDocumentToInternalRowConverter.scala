package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.SearchDocument
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

case class SearchDocumentToInternalRowConverter(private val schema: StructType)
  extends (SearchDocument => InternalRow) {

  override def apply(v1: SearchDocument): InternalRow = {

    val values: Seq[Any] = schema.map {
      sf =>
        ReadConverters.converterForType(sf.dataType)
          .apply(v1.get(sf.name))
    }

    InternalRow(values: _*)
  }
}
