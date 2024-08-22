package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.SearchDocument
import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{ArrayType, DataTypes, StructType}

case class SearchDocumentToInternalRowConverter(private val schema: StructType)
  extends (SearchDocument => InternalRow) {

  override def apply(v1: SearchDocument): InternalRow = {

    val values: Seq[Any] = schema.map {
      sf =>
        val readConverter: ReadConverter[_] = sf.dataType match {
          case DataTypes.StringType => ReadConverters.StringConverter
          case DataTypes.IntegerType => ReadConverters.IntegerConverter
          case DataTypes.LongType => ReadConverters.LongConverter
          case DataTypes.DoubleType => ReadConverters.DoubleConverter
          case DataTypes.FloatType => ReadConverters.FloatConverter
          case DataTypes.BooleanType => ReadConverters.BooleanConverter
          case DataTypes.DateType => ReadConverters.DateConverter
          case DataTypes.TimestampType => ReadConverters.TimestampConverter
          case struct: StructType => ReadConverters.ComplexConverter(struct)
          case array: ArrayType => ReadConverters.ArrayConverter(array.elementType)
          case _ => throw new AzureSparkException(s"Unsupported read type (${sf.dataType})")
        }

        readConverter.toInternal(v1.get(sf.name))
    }

    InternalRow(values: _*)
  }
}
