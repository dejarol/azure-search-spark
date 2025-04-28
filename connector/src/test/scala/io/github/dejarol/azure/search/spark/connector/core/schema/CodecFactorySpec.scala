package io.github.dejarol.azure.search.spark.connector.core.schema

import io.github.dejarol.azure.search.spark.connector.{BasicSpec, FieldFactory}
import org.json4s.jackson.JsonMethods.pretty
import org.json4s.{JValue, JsonAST}
import org.scalatest.EitherValues

/**
 * Mix-in trait for testing subclasses of [[CodecFactory]]
 */

trait CodecFactorySpec
  extends BasicSpec
    with FieldFactory
      with EitherValues {

  // TODO: document
  protected final def jValueAsJSONString(value: JValue): String = pretty(value)

  protected final def codecErrorAsJSONString(error: CodecError): String = jValueAsJSONString(error.toJValue)

  protected final def jObjectFields(value: JValue): Map[String, JValue] = {

    value match {
      case JsonAST.JObject(obj) => obj.toMap
      case _ => Map.empty
    }
  }
}
