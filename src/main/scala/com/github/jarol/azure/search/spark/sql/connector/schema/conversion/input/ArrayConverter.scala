package com.github.jarol.azure.search.spark.sql.connector.schema.conversion.input

import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import org.apache.spark.sql.catalyst.util.ArrayData

/**
 * Converter for arrays
 * @param internalConverter converter for internal type
 */

case class ArrayConverter(private val internalConverter: SparkInternalConverter)
  extends SparkInternalTransformConverter[ArrayData] {

  override def transform(value: Any): ArrayData = {

    val values: Seq[Any] = JavaScalaConverters
      .listToSeq(value.asInstanceOf[java.util.List[Object]])
      .map(internalConverter.toSparkInternalObject)

    ArrayData.toArrayData(values)
  }
}
