package io.github.dejarol.azure.search.spark.connector.core.utils

import scala.reflect.ClassTag

/**
 * Collection of utilities for dealing with reflection
 */

object Reflection {

  /**
   * Retrieve the class of a type from its class tag
   * @tparam C class tag type
   * @return the type class
   */

  final def classFromClassTag[C: ClassTag]: Class[C] = {

    implicitly[ClassTag[C]]
      .runtimeClass
      .asInstanceOf[Class[C]]
  }
}
