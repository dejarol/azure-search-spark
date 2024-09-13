package com.github.jarol.azure.search.spark.sql.connector

import java.util
import scala.collection.JavaConverters._

object JavaScalaConverters {

  /**
   * Convert a Scala seq to a [[java.util.List]]
   * @param seq Scala seq
   * @tparam T seq type
   * @return a java list
   */

  def seqToList[T](seq: Seq[T]): util.List[T] = seq.asJava

  /**
   * Convert a java list to a seq
   * @param list java list
   * @tparam T list type
   * @return a seq
   */

  def listToSeq[T](list: util.List[T]): Seq[T] = list.asScala

  /**
   * Convert a java map to a scala map
   * @param map map
   * @tparam K key type
   * @tparam V value type
   * @return a scala immutable map
   */

  def javaMapToScala[K, V](map: util.Map[K, V]): Map[K, V] = map.asScala.toMap

  /**
   * Convert a Scala map to a Java map
   * @param map map
   * @tparam K key type
   * @tparam V value type
   * @return a scala immutable map
   */

  def scalaMapToJava[K, V](map: Map[K, V]): util.Map[K, V] = map.asJava
}
