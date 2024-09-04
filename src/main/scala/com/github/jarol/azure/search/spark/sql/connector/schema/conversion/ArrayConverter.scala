package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import org.apache.spark.sql.catalyst.util.ArrayData

case class ArrayConverter(private val internalConverter: SparkInternalConverter)
  extends SparkInternalConverter {

  override def toSparkInternalObject(value: Any): ArrayData = {

    val values: Seq[Any] = JavaScalaConverters
      .listToSeq(value.asInstanceOf[java.util.List[Object]])
      .map(internalConverter.toSparkInternalObject)

    ArrayData.toArrayData(values)
  }
}
