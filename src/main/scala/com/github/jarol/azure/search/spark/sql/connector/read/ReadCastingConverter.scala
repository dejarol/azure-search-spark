package com.github.jarol.azure.search.spark.sql.connector.read

import java.util.Objects

/**
 * Read converter that simply applies a casting to given value
 * @tparam T output value type
 */

trait ReadCastingConverter[T]
  extends ReadConverter[T] {

  override final def toInternal(obj: Object): T = {
    obj.asInstanceOf[T]
  }
}

/**
 * Read converter that
 *  - first checks that the value is not null
 *  - and then applies a transformation on such value
 *
 *  Suitable for cases when the input value should be transformed by a function that does not accept null values
 * @tparam T output value type
 */

trait ReadNullableConverter[T]
  extends ReadConverter[T] {

  override final def toInternal(obj: Object): T = {

    if (Objects.isNull(obj)) {
      null.asInstanceOf[T]
    } else {
      transform(obj)
    }
  }

  /**
   * Transform the value into an instance of the output type
   * @param obj input object
   * @return an instance of the output type
   */

  protected def transform(obj: Object): T
}