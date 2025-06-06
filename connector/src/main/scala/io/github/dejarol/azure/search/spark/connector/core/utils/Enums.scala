package io.github.dejarol.azure.search.spark.connector.core.utils

import scala.reflect.ClassTag

/**
 * Collection of utility methods for dealing with Java enums
 */

object Enums {

  /**
   * Safely gets the first value of an enum that matches a predicate
   * @param value string value
   * @param predicate predicate
   * @tparam E enum type
   * @return the first enum value that matches the predicate
   */

  final def safeValueOf[E <: Enum[E]: ClassTag](value: String, predicate: (E, String) => Boolean): Option[E] = {

    Reflection.classFromClassTag[E]
      .getEnumConstants
      .find(predicate(_, value))
  }

  /**
   * Gets the first element of an enum matching a predicate, or throws an exception
   * @param value string value
   * @param predicate predicate to match
   * @tparam E enum type
   * @return the first enum value that matches the predicate
   */

  @throws[NoSuchElementException]
  final def unsafeValueOf[E <: Enum[E]: ClassTag](value: String, predicate: (E, String) => Boolean): E = {

    safeValueOf[E](value, predicate) match {
      case Some(value) => value
      case None => throw new NoSuchElementException(
        s"Could not find a matching value on enum ${Reflection.classFromClassTag[E].getName}"
      )
    }
  }
}
