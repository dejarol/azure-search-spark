package io.github.jarol.azure.search.spark.connector.models

import io.github.jarol.azure.search.spark.connector.core.JavaScalaConverters
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
   * Get a document property
   * @param key key
   * @tparam T property type
   * @return the value related to requested key
   */

  def getProperty[T: PropertyDeserializer](key: String): T = {

    implicitly[PropertyDeserializer[T]].deserialize(
      delegate.get(key)
    )
  }

  /**
   * Get an array property
   * @param key key
   * @tparam T array type (should have an implicit [[PropertyDeserializer]] in scope)
   * @return the array value for given key
   */

  def getArray[T: PropertyDeserializer](key: String): Seq[T] = {

    val deserializer = implicitly[PropertyDeserializer[T]]
    JavaScalaConverters.listToSeq(
      delegate.get(key).asInstanceOf[JList[AnyRef]]
    ).map {
      deserializer.deserialize
    }
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
   * Maybe get the value of a key
   * @param key key
   * @tparam T value type
   * @return a non-empty Option for a non-null value
   */

  def maybeGetProperty[T: PropertyDeserializer](key: String): Option[T] = {

    Option(delegate.get(key)).map {
      v => implicitly[PropertyDeserializer[T]].deserialize(v)
    }
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

  /**
   * Get an optional collection of values
   * @param key key
   * @tparam T collection inner type
   * @return a non-empty Option if the property is defined
   */

  def maybeGetArray[T: PropertyDeserializer](key: String): Option[Seq[T]] = {

    Option(delegate.get(key)).map {
      _ => getArray[T](key)
    }
  }
}
