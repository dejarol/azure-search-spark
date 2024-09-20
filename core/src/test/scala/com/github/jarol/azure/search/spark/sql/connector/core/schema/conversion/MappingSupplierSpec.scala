package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}
import org.apache.spark.sql.types.StructField
import org.scalatest.EitherValues

/**
 * Parent class for testing subclasses of [[SafeMappingSupplier]]
 * @param supplier supplier to test
 * @tparam K supplier's mapping key type
 * @tparam V supplier's mapping value type
 */

abstract class MappingSupplierSpec[K, V](protected val supplier: SafeMappingSupplier[K, V])
  extends BasicSpec
    with FieldFactory
      with EitherValues {

  protected final lazy val (first, second, third, fourth) = ("first", "second", "third", "fourth")

  /**
   * Assert the existence of a [[SearchPropertyConverter]] for given Spark and Search field
   * @param structField Spark field
   * @param searchField Search field
   * @param shouldExists true for converter that should exist
   */

  private def assertConverterExistence(
                                        structField: StructField,
                                        searchField: SearchField,
                                        shouldExists: Boolean
                                      ): Unit = {

    val maybeConverter = supplier.getConverter(
      structField, searchField
    )

    if (shouldExists) {
      maybeConverter shouldBe defined
    } else {
      maybeConverter shouldBe empty
    }
  }

  /**
   * Assert that no converter exists for given Spark and Search fields
   * @param structField Spark field
   * @param searchField Search field
   */

  protected final def assertNoConverterExists(structField: StructField, searchField: SearchField): Unit = {

    assertConverterExistence(
      structField,
      searchField,
      shouldExists = false
    )
  }

  /**
   * Assert that a converter exists for given Spark and Search fields
   * @param structField Spark field
   * @param searchField Search field
   */

  protected final def assertConverterExists(structField: StructField, searchField: SearchField): Unit = {

    assertConverterExistence(
      structField,
      searchField, shouldExists = true
    )
  }

  /**
   * Assert that the mapping is a left (i.e. some schema incompatibilities were found)
   * @param spark Spark fields
   * @param search Search fields
   */

  protected final def assertMappingIsLeft(spark: Seq[StructField], search: Seq[SearchField]): Unit = {

    val either = supplier.getMapping(
      spark,
      search,
      "index"
    )

    either shouldBe 'left
  }
}
