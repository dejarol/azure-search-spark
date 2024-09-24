package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}
import org.apache.spark.sql.types.StructField
import org.scalatest.EitherValues

/**
 * Parent class for testing subclasses of [[SafeMappingSupplier]]
 * @param supplier mapping supplier
 * @tparam C supplier type
 */

abstract class MappingSupplierSpec[C](protected val supplier: SafeMappingSupplier[C])
  extends BasicSpec
    with FieldFactory
      with EitherValues {

  protected final lazy val (first, second, third, fourth) = ("first", "second", "third", "fourth")

  /**
   * Assert that the result returned by the supplier is a [[Left]]
   * @param schema Spark schema
   * @param searchFields Search fields
   */

  protected final def assertIsLeft(schema: Seq[StructField], searchFields: Seq[SearchField]): Unit = {

    supplier.get(
      schema,
      searchFields,
      "hello"
    ) should be ('left)
  }

  /**
   * Assert that the result returned by the supplier is a [[Right]]
   * @param schema Spark schema
   * @param searchFields Search fields
   */

  protected final def assertIsRight(schema: Seq[StructField], searchFields: Seq[SearchField]): Unit = {

    supplier.get(
      schema,
      searchFields,
      "hello"
    ) should be ('right)
  }
}
