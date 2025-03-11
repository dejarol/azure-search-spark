package io.github.dejarol.azure.search.spark.connector.core.schema

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

  def expectedAtomic: Boolean

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

object SearchFieldTypeAssertion {

  /**
   * Assertion for simple types
   */

  case object Atomic
    extends SearchFieldTypeAssertion {

    override def predicate: SearchFieldDataType => Boolean = _.isAtomic
    override def expectedAtomic: Boolean = true
    override def expectedComplex: Boolean = false
    override def expectedCollection: Boolean = false
    override def expectedGeoPoint: Boolean = false
  }

  /**
   * Assertion for collection types
   */

  case object Collection extends SearchFieldTypeAssertion {
    override def predicate: SearchFieldDataType => Boolean = _.isCollection
    override def expectedAtomic: Boolean = false
    override def expectedComplex: Boolean = false
    override def expectedCollection: Boolean = true
    override def expectedGeoPoint: Boolean = false
  }

  /**
   * Assertions for complex types
   */

  case object Complex extends SearchFieldTypeAssertion {
    override def predicate: SearchFieldDataType => Boolean = _.isComplex
    override def expectedAtomic: Boolean = false
    override def expectedComplex: Boolean = true
    override def expectedCollection: Boolean = false
    override def expectedGeoPoint: Boolean = false
  }

  case object GeoPoint extends SearchFieldTypeAssertion {
    override def predicate: SearchFieldDataType => Boolean = _.isGeoPoint
    override def expectedAtomic: Boolean = false
    override def expectedComplex: Boolean = false
    override def expectedCollection: Boolean = false
    override def expectedGeoPoint: Boolean = true
  }
}
