package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}
import org.apache.spark.sql.types.StructField

import scala.reflect.ClassTag

abstract class SafeConverterSupplierSpec[C](protected val supplier: SafeConverterSupplier[C])
  extends BasicSpec
    with FieldFactory {

  protected final lazy val (first, second, third, fourth) = ("first", "second", "third", "fourth")

  /**
   * Assert that a converter exists for such field definitions and matches a given type
   * @param structField Spark field
   * @param searchField Search field
   * @tparam TConverter converter type
   */

  protected final def assertConverterExistsAndIsA[TConverter: ClassTag](
                                                                         structField: StructField,
                                                                         searchField: SearchField
                                                                       ): Unit = {

    val maybe = supplier.get(structField, searchField)
    maybe shouldBe defined
    maybe.get shouldBe a[TConverter]
  }

  /**
   * Assert that no converter exists for given Spark and Search fields
   * @param structField Spark field
   * @param searchField Search field
   */

  protected final def assertNoConverterExists(structField: StructField, searchField: SearchField): Unit = {

    supplier.get(structField, searchField) shouldBe empty
  }
}
