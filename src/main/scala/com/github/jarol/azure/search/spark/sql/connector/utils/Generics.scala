package com.github.jarol.azure.search.spark.sql.connector.utils

import scala.reflect.ClassTag

object Generics {

  /**
   * Retrieve the class of a type from its class tag
   * @tparam C class tag type
   * @return the type class
   */

  private final def classFromClassTag[C: ClassTag]: Class[C] = implicitly[ClassTag[C]].runtimeClass.asInstanceOf[Class[C]]

  /**
   * Safely get the first value of an enum that matches a predicate
   * @param value string value
   * @param predicate predicate
   * @tparam E enum type
   * @return the first enum value that matches the predicate
   */

  final def safeValueOfEnum[E <: Enum[E]: ClassTag](value: String, predicate: (E, String) => Boolean): Option[E] = {

    classFromClassTag[E].getEnumConstants.collectFirst {
      case e if predicate(e, value) => e
    }
  }

  /**
   * Get the first element of an enum matching a predicate, or throw an exception
   * @param value string value
   * @param predicate predicate to match
   * @tparam E enum type
   * @throws NoSuchElementException if no value matches
   * @return the first enum value that matches the predicate
   */

  @throws[NoSuchElementException]
  final def unsafeValueOfEnum[E <: Enum[E]: ClassTag](value: String, predicate: (E, String) => Boolean): E = {

    safeValueOfEnum[E](value, predicate) match {
      case Some(value) => value
      case None => throw new NoSuchElementException(
        s"Could not find a matching value on enum ${classFromClassTag[E].getName}"
      )
    }
  }

  final def partialFunction[A, B](predicate: A => Boolean, function: A => B): PartialFunction[A, B] = {

    case a: A if predicate(a) => function(a)
  }
}
