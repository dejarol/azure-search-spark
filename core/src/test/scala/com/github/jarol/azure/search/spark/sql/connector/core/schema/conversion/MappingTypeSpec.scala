package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion

import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}

import scala.reflect.ClassTag

trait MappingTypeSpec[K, V]
  extends BasicSpec
    with FieldFactory {

  protected final lazy val (first, second, third, fourth) = ("first", "second", "third", "fourth")

  protected def assertDefinedAndAnInstanceOf[T: ClassTag](opt: Option[_]): Unit = {

    opt shouldBe defined
    opt.get shouldBe a [T]
  }
}
