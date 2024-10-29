package com.github.jarol.azure.search.spark.sql.connector.models

trait PropertySerializer[T] {

  def toPropertyValue(v1: T): AnyRef
}
