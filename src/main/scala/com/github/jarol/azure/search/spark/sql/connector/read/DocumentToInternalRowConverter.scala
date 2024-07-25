package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.SearchDocument
import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.unsafe.types.UTF8String

case class DocumentToInternalRowConverter(private val schema: StructType,
                                          private val readConfig: ReadConfig)
  extends (SearchDocument => InternalRow) {

  override def apply(v1: SearchDocument): InternalRow = {

    val values: Seq[Any] = schema.map {
      sf =>
        val obj = v1.get(sf.name)
        sf.dataType match {
          case DataTypes.StringType => UTF8String.fromString(obj.asInstanceOf[String])
          case DataTypes.IntegerType => obj.asInstanceOf[Integer]
          case DataTypes.BooleanType => obj.asInstanceOf[java.lang.Boolean]
          case _ => obj
        }
    }

    InternalRow(values: _*)
  }
}
