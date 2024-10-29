package com.github.jarol.azure.search.spark.sql.connector.models

import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters

import java.util.{List => JList, Map => JMap}

class MapOperations(private val delegate: JMap[String, AnyRef]) {

  def addProperty[T: PropertySerializer](key: String, value: T): JMap[String, AnyRef] = {

    val mapper = implicitly[PropertySerializer[T]]
    delegate.put(key, mapper.toPropertyValue(value))
    delegate
  }

  def maybeAddProperty[T: PropertySerializer](key: String, value: Option[T]): JMap[String, AnyRef] = {

    value.map {
      addProperty(key, _)
    }.getOrElse(delegate)
  }

  def addArray[T: PropertySerializer](key: String, value: Seq[T]): JMap[String, AnyRef] = {

    val mapper = implicitly[PropertySerializer[T]]
    val array: JList[AnyRef] = JavaScalaConverters.seqToList(
      value.map(mapper.toPropertyValue)
    )

    delegate.put(key, array)
    delegate
  }

  def maybeAddArray[T: PropertySerializer](key: String, value: Option[Seq[T]]): JMap[String, AnyRef] = {

    value.map {
      addArray(key, _)
    }.getOrElse(delegate)
  }

  def addSubDocument(key: String, value: JMap[String, AnyRef]): JMap[String, AnyRef] = {

    delegate.put(key, value)
    delegate
  }
}
