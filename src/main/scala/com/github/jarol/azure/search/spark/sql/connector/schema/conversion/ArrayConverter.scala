package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import org.apache.spark.sql.catalyst.util.ArrayData

case class ArrayConverter(private val internal: SearchFieldDataType)
  extends SparkInternalConverter {

  override def toSparkInternalObject(value: Any): ArrayData = {

    val values: Seq[Any] = internal match {
      case SearchFieldDataType.STRING => castAsSeqOf[String](value)
      case SearchFieldDataType.INT32 => castAsSeqOf[java.lang.Integer](value)
      case SearchFieldDataType.INT64 => castAsSeqOf[java.lang.Long](value)
      case SearchFieldDataType.DOUBLE => castAsSeqOf[java.lang.Double](value)
    }
    ArrayData.toArrayData(values)
  }

  private def castAsSeqOf[T](value: Any): Seq[T] = {

    JavaScalaConverters.listToSeq(
      value.asInstanceOf[java.util.List[T]]
    )
  }
}
