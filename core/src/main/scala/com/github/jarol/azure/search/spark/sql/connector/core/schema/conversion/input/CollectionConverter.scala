package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input

import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import org.apache.spark.sql.catalyst.util.ArrayData

/**
 * Converter for Search collections
 * @param internal converter for collection internal objects
 */

case class CollectionConverter(private val internal: ReadConverter)
  extends ReadTransformConverter[ArrayData] {

  override protected def transform(value: Any): ArrayData = {

    val values: Seq[Any] = JavaScalaConverters
      .listToSeq(value.asInstanceOf[java.util.List[Object]])
      .map(internal.apply)

    ArrayData.toArrayData(values)
  }
}
