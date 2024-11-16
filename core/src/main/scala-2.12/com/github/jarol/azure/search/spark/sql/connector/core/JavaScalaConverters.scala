package com.github.jarol.azure.search.spark.sql.connector.core

import java.util.{List => JList, Map => JMap}
import scala.collection.JavaConverters._

/**
 * Object holding conversion methods back and forth Java and Scala collections
 */

object JavaScalaConverters {

  /**
   * Convert a Scala seq to a Java list
   * @param seq Scala seq
   * @tparam T seq type
   * @return a Java list
   */

  def seqToList[T](seq: Seq[T]): JList[T] = seq.asJava

  /**
   * Convert a Java list to a seq
   * @param list java list
   * @tparam T list type
   * @return a seq
   */

  def listToSeq[T](list: JList[T]): Seq[T] = list.asScala

  /**
   * Convert a Java map to a Scala map
   * @param map map
   * @tparam K key type
   * @tparam V value type
   * @return a scala immutable map
   */

  def javaMapToScala[K, V](map: JMap[K, V]): Map[K, V] = map.asScala.toMap

  /**
   * Convert a Scala map to a Java map
   * @param map Scala map
   * @tparam K key type
   * @tparam V value type
   * @return a Java map
   */

  def scalaMapToJava[K, V](map: Map[K, V]): JMap[K, V] = map.asJava
}
