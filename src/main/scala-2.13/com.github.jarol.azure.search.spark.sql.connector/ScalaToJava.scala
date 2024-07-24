package com.github.jarol.azure.search.spark.sql.connector

import java.util
import scala.jdk.CollectionConverters._

object ScalaToJava {

  def set[T](set: Set[T]): util.Set[T] = set.asJava
}