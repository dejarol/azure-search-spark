package com.github.jarol.azure.search.spark.sql.connector

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.reflect.runtime.universe.{TypeTag, typeOf}

trait BasicSpec
  extends AnyFunSpec
    with Matchers {

  protected final val SHOULD = "should"
  protected final val SHOULD_NOT = "should not"

  protected final def AnInstanceOf[T: TypeTag]: String = s"An instance of class ${nameOf[T]}"

  protected final def TheCompanionObject[T: TypeTag]: String = s"The companion object ${nameOf[T]}"

  protected final def nameOf[T: TypeTag]: String = typeOf[T].typeSymbol.name.toString
}
