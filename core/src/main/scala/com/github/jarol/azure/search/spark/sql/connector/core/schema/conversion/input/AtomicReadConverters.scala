package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input

import java.lang

/**
 * Converters for Search atomic types
 */

object AtomicReadConverters {

  case object Int32ToLongConverter
    extends ReadTransformConverter[lang.Long] {

    override protected def transform(value: Any): lang.Long = {

      value.asInstanceOf[lang.Integer].longValue()
    }
  }

  case object Int64ToIntConverter
    extends ReadTransformConverter[lang.Integer] {

    override protected def transform(value: Any): Integer = {

      value.asInstanceOf[lang.Long].intValue()
    }
  }
}
