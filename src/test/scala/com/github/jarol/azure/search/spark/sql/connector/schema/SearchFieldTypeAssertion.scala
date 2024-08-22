package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.SearchFieldDataType

/**
 * Assertion for testing the type of a [[SearchFieldDataType]]
 */

trait SearchFieldTypeAssertion {

  /**
   * The predicate that should be used for testing a [[SearchFieldDataType]]
   * @return predicate for testing
   */

  def predicate: SearchFieldDataType => Boolean

  /**
   * Return the expected result of testing a simple [[SearchFieldDataType]] using this instance's predicate
   * @return true if this instance's predicate should return true for simple types
   */

  def expectedSimple: Boolean

  /**
   * Return the expected result of testing a complex [[SearchFieldDataType]] using this instance's predicate
   * @return true if this instance's predicate should return true for complex types
   */

  def expectedComplex: Boolean

  /**
   * Return the expected result of testing a collection [[SearchFieldDataType]] using this instance's predicate
   * @return true if this instance's predicate should return true for collection types
   */

  def expectedCollection: Boolean

  /**
   * Return the expected result of testing a geoPoint [[SearchFieldDataType]] using this instance's predicate
   * @return true if this instance's predicate should return true for geoPoint types
   */

  def expectedGeoPoint: Boolean
}

object TypeAssertions {

  /**
   * Assertion for simple types
   */

  case object Simple
    extends SearchFieldTypeAssertion {

    override def predicate: SearchFieldDataType => Boolean = SchemaUtils.isSimpleType
    override def expectedSimple: Boolean = true
    override def expectedComplex: Boolean = false
    override def expectedCollection: Boolean = false
    override def expectedGeoPoint: Boolean = false
  }

  /**
   * Assertion for collection types
   */

  case object Collection extends SearchFieldTypeAssertion {
    override def predicate: SearchFieldDataType => Boolean = SchemaUtils.isCollectionType
    override def expectedSimple: Boolean = false
    override def expectedComplex: Boolean = false
    override def expectedCollection: Boolean = true
    override def expectedGeoPoint: Boolean = false
  }

  /**
   * Assertions for complex types
   */

  case object Complex extends SearchFieldTypeAssertion {
    override def predicate: SearchFieldDataType => Boolean = SchemaUtils.isComplexType
    override def expectedSimple: Boolean = false
    override def expectedComplex: Boolean = true
    override def expectedCollection: Boolean = false
    override def expectedGeoPoint: Boolean = false
  }

  case object GeoPoint extends SearchFieldTypeAssertion {
    override def predicate: SearchFieldDataType => Boolean = SchemaUtils.isGeoPoint
    override def expectedSimple: Boolean = false
    override def expectedComplex: Boolean = false
    override def expectedCollection: Boolean = false
    override def expectedGeoPoint: Boolean = true
  }
}
