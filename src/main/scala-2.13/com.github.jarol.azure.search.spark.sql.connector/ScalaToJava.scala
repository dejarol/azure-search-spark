package com.github.jarol.azure.search.spark.sql.connector

import java.util
import scala.jdk.CollectionConverters._

object ScalaToJava {

  /**
   * Convert a Scala set to a [[java.util.Set]]
   * @param set Scala set
   * @tparam T set type
   * @return a java set
   */

  def set[T](set: Set[T]): util.Set[T] = set.asJava

  /**
   * Convert a Scala seq to a [[java.util.List]]
   * @param seq Scala seq
   * @tparam T seq type
   * @return a java list
   */

  def seqToList[T](seq: Seq[T]): util.List[T] = seq.asJava
}