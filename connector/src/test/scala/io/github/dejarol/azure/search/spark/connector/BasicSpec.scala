package io.github.dejarol.azure.search.spark.connector

import org.scalatest.Inspectors
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.reflect.runtime.universe.{TypeTag, typeOf}

/**
 * Parent class for all specs
 */

trait BasicSpec
  extends AnyFunSpec
    with Matchers with Inspectors {

  protected final val SHOULD = "should"
  protected final val SHOULD_NOT = "should not"

  /**
   * Given a class
   * {{{
   * class MySpecialClass
   * }}}
   * returns the string
   * {{{
   * "An instance of class MySpecialClass"
   * }}}
   * @tparam T class TypeTag
   * @return a string to use as suite title or subtitle
   */

  protected final def anInstanceOf[T: TypeTag]: String = s"An instance of ${nameOf[T]}"

  /**
   * Given an object
   * {{{
   * object MySpecialObject
   * }}}
   * returns the string
   * {{{
   * "Object MySpecialObject"
   * }}}
   * @tparam T object TypeTag
   * @return a string to use as suite title or subtitle
   */

  protected final def `object`[T: TypeTag]: String = s"Object ${nameOf[T]}"

  /**
   * Return the class name of a TypeTag
   * @tparam T TypeTag
   * @return the class name of a TypeTag
   */

  protected final def nameOf[T: TypeTag]: String = typeOf[T].typeSymbol.name.toString
}
