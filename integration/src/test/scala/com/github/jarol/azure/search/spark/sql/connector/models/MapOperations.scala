package com.github.jarol.azure.search.spark.sql.connector.models

import com.github.jarol.azure.search.spark.sql.connector.PropertySerializer
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters

import java.util.{List => JList, Map => JMap}

/**
 * Wrapper for enriching a Java Map with some utility methods
 * @param delegate delegate map
 */

class MapOperations(private val delegate: JMap[String, AnyRef]) {

  /**
   * Add a property
   * @param key key
   * @param value value
   * @tparam T value type
   * @return this map plus a new key-value pair
   */

  def addProperty[T: PropertySerializer](key: String, value: T): JMap[String, AnyRef] = {

    val mapper = implicitly[PropertySerializer[T]]
    delegate.put(key, mapper.serialize(value))
    delegate
  }

  /**
   * Add an optional property, if defined
   * @param key key
   * @param value optional property
   * @tparam T property type
   * @return this map as-is if the property is empty, or this map plus a new key-value pair
   */

  def maybeAddProperty[T: PropertySerializer](key: String, value: Option[T]): JMap[String, AnyRef] = {

    value.map {
      addProperty(key, _)
    }.getOrElse(delegate)
  }

  /**
   * Add an array property
   * @param key key
   * @param value value (a collection)
   * @tparam T value collection type
   * @return this map plus a new key-value pair
   */

  def addArray[T: PropertySerializer](key: String, value: Seq[T]): JMap[String, AnyRef] = {

    val mapper = implicitly[PropertySerializer[T]]
    val array: JList[AnyRef] = JavaScalaConverters.seqToList(
      value.map(mapper.serialize)
    )

    delegate.put(key, array)
    delegate
  }

  /**
   * Add an optional array property, if defined
   * @param key key
   * @param value optional value
   * @tparam T property collection type
   * @return this map as-is if the property is empty, or this map plus a new key-value pair
   */

  def maybeAddArray[T: PropertySerializer](key: String, value: Option[Seq[T]]): JMap[String, AnyRef] = {

    value.map {
      addArray(key, _)
    }.getOrElse(delegate)
  }
}
