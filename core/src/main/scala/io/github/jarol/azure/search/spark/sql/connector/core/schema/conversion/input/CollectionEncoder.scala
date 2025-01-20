package io.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input

import io.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import org.apache.spark.sql.catalyst.util.ArrayData

import java.lang.{Object => JObject}
import java.util.{List => JList}

/**
 * Encoder for Search collections
 * @param internal encoder for collection internal objects
 */

case class CollectionEncoder(private val internal: SearchEncoder)
  extends TransformEncoder[ArrayData] {

  override protected def transform(value: JObject): ArrayData = {

    val values: Seq[Any] = JavaScalaConverters
      .listToSeq(value.asInstanceOf[JList[Object]])
      .map(internal.apply)

    ArrayData.toArrayData(values)
  }
}
